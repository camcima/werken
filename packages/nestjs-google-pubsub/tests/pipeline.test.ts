import { describe, expect, test, vi } from "vitest";
import { MessagePipeline } from "@werken/nestjs-google-pubsub/internal";
import type { CloudEventContext, IncomingMessage } from "@werken/nestjs-google-pubsub";

const TYPE = "com.example.thing.happened.v1";

function message(overrides: Partial<IncomingMessage> = {}): IncomingMessage {
  return {
    id: "pubsub-message-1",
    attributes: {
      "ce-specversion": "1.0",
      "ce-id": "01931b7c-3f2a-7000-8000-000000000001",
      "ce-source": "https://example.test/service",
      "ce-type": TYPE,
    },
    data: Buffer.from(JSON.stringify({ hello: "world" })),
    publishTime: new Date("2026-08-02T15:00:00.000Z"),
    deliveryAttempt: 0,
    orderingKey: "",
    ack: () => {},
    nack: () => {},
    ...overrides,
  };
}

function pipelineWith(handlers: Record<string, (data: unknown, ctx: CloudEventContext) => unknown>) {
  return new MessagePipeline({
    subscription: "projects/p/subscriptions/s",
    resolveRoute: (type) => (handlers[type] === undefined ? null : { handler: handlers[type], pattern: type }),
  });
}

describe("routing", () => {
  test("invokes the handler whose pattern equals ce-type", async () => {
    const handler = vi.fn();
    const outcome = await pipelineWith({ [TYPE]: handler }).handle(message());

    expect(handler).toHaveBeenCalledTimes(1);
    expect(outcome).toBe("ack");
  });

  test("passes the decoded JSON payload and a populated context", async () => {
    let seenData: unknown;
    let seenCtx: CloudEventContext | undefined;
    await pipelineWith({
      [TYPE]: (data, ctx) => {
        seenData = data;
        seenCtx = ctx;
      },
    }).handle(message());

    expect(seenData).toEqual({ hello: "world" });
    expect(seenCtx?.type).toBe(TYPE);
    expect(seenCtx?.messageId).toBe("pubsub-message-1");
  });

  test("does not invoke a handler registered under a different type", async () => {
    const other = vi.fn();
    await pipelineWith({ "com.example.other.v1": other }).handle(message());

    expect(other).not.toHaveBeenCalled();
  });

  // onUnhandledPattern defaults to 'ack' — a subscription legitimately receives types this
  // consumer does not care about, and nacking them would spin forever.
  test("acks a message no handler matches", async () => {
    expect(await pipelineWith({}).handle(message())).toBe("ack");
  });
});

describe("outcomes", () => {
  test("acks when the handler resolves", async () => {
    expect(await pipelineWith({ [TYPE]: async () => {} }).handle(message())).toBe("ack");
  });

  test("nacks when the handler throws", async () => {
    const outcome = await pipelineWith({
      [TYPE]: () => {
        throw new Error("transient");
      },
    }).handle(message());

    expect(outcome).toBe("nack");
  });

  test("nacks when the handler rejects", async () => {
    const outcome = await pipelineWith({ [TYPE]: async () => Promise.reject(new Error("transient")) }).handle(
      message(),
    );

    expect(outcome).toBe("nack");
  });

  test("waits for an async handler before deciding the outcome", async () => {
    let finished = false;
    const outcome = await pipelineWith({
      [TYPE]: async () => {
        await new Promise((r) => setTimeout(r, 10));
        finished = true;
      },
    }).handle(message());

    expect(finished).toBe(true);
    expect(outcome).toBe("ack");
  });
});

// Nest wraps controller handlers so they return an Observable rather than a plain value. Awaiting
// one is a no-op, so without explicit subscription a throwing handler would ack and lose the
// message. These cover the wrapper shape directly, structurally, without pulling in rxjs.
describe("Observable-returning handlers", () => {
  const observableOf = (behaviour: (o: { error: (e: unknown) => void; complete: () => void }) => void) => ({
    subscribe: (observer: { error: (e: unknown) => void; complete: () => void }) => {
      behaviour(observer);
      return { unsubscribe() {} };
    },
  });

  test("nacks when the returned Observable errors", async () => {
    const outcome = await pipelineWith({
      [TYPE]: () => observableOf((o) => o.error(new Error("handler blew up"))),
    }).handle(message());

    expect(outcome).toBe("nack");
  });

  test("acks when the returned Observable completes", async () => {
    const outcome = await pipelineWith({ [TYPE]: () => observableOf((o) => o.complete()) }).handle(message());

    expect(outcome).toBe("ack");
  });

  test("waits for the Observable to complete before deciding", async () => {
    let completed = false;
    const outcome = await pipelineWith({
      [TYPE]: () =>
        observableOf((o) => {
          setTimeout(() => {
            completed = true;
            o.complete();
          }, 10);
        }),
    }).handle(message());

    expect(completed).toBe(true);
    expect(outcome).toBe("ack");
  });

  test("nacks when a Promise resolves to an erroring Observable, as Nest returns", async () => {
    const outcome = await pipelineWith({
      [TYPE]: async () => observableOf((o) => o.error(new Error("async wrapper"))),
    }).handle(message());

    expect(outcome).toBe("nack");
  });
});

describe("malformed input", () => {
  // Envelope and decode policies arrive in M4; until dead-lettering exists, nack is the outcome
  // that cannot lose a message.
  test("nacks a message whose envelope is invalid", async () => {
    const bad = message({ attributes: { "ce-specversion": "1.0" } });

    expect(await pipelineWith({ [TYPE]: () => {} }).handle(bad)).toBe("nack");
  });

  test("nacks a message whose body is not valid JSON", async () => {
    const bad = message({ data: Buffer.from("{not json") });

    expect(await pipelineWith({ [TYPE]: () => {} }).handle(bad)).toBe("nack");
  });

  test("treats an empty body as an undefined payload rather than a parse failure", async () => {
    let seenData: unknown = "untouched";
    const outcome = await pipelineWith({
      [TYPE]: (data) => {
        seenData = data;
      },
    }).handle(message({ data: Buffer.alloc(0) }));

    expect(seenData).toBeUndefined();
    expect(outcome).toBe("ack");
  });

  test("nacks when a handler throws a non-Error value", async () => {
    const outcome = await pipelineWith({
      [TYPE]: () => {
        throw "just a string";
      },
    }).handle(message());

    expect(outcome).toBe("nack");
  });

  test("does not invoke the handler when the envelope is invalid", async () => {
    const handler = vi.fn();
    await pipelineWith({ [TYPE]: handler }).handle(message({ attributes: { "ce-specversion": "1.0" } }));

    expect(handler).not.toHaveBeenCalled();
  });
});
