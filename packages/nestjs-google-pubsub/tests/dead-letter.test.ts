import { describe, expect, test, vi } from "vitest";
import { MessagePipeline, TerminalEventError } from "@werken/nestjs-google-pubsub";
import type { CloudEventContext, DeadLetterRequest, IncomingMessage } from "@werken/nestjs-google-pubsub";

const TYPE = "com.example.thing.happened.v1";
const SUBSCRIPTION = "projects/p/subscriptions/s";

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

function recordingPublisher(behaviour?: () => Promise<void>) {
  const published: DeadLetterRequest[] = [];
  return {
    published,
    publisher: {
      publish: vi.fn(async (request: DeadLetterRequest) => {
        published.push(request);
        if (behaviour) await behaviour();
      }),
    },
  };
}

function pipelineWith(
  handlers: Record<string, (data: unknown, ctx: CloudEventContext) => unknown>,
  extra: Partial<ConstructorParameters<typeof MessagePipeline>[0]> = {},
) {
  return new MessagePipeline({
    subscription: SUBSCRIPTION,
    resolveHandler: (pattern) => handlers[pattern] ?? null,
    ...extra,
  });
}

describe("TerminalEventError", () => {
  test("dead-letters instead of burning the retry budget", async () => {
    const { publisher, published } = recordingPublisher();
    const outcome = await pipelineWith(
      {
        [TYPE]: () => {
          throw new TerminalEventError("unknown reference");
        },
      },
      { deadLetterPublisher: publisher },
    ).handle(message());

    expect(outcome).toBe("dead-letter");
    expect(published).toHaveLength(1);
    expect(published[0].reason).toContain("unknown reference");
    expect(published[0].stage).toBe("handler");
  });

  test("carries structured detail through to the publisher", async () => {
    const { publisher, published } = recordingPublisher();
    await pipelineWith(
      {
        [TYPE]: () => {
          throw new TerminalEventError("unknown reference", { ref: "abc" });
        },
      },
      { deadLetterPublisher: publisher },
    ).handle(message());

    expect(published[0].detail).toEqual({ ref: "abc" });
  });

  test("a generic error still nacks for redelivery", async () => {
    const { publisher, published } = recordingPublisher();
    const outcome = await pipelineWith(
      {
        [TYPE]: () => {
          throw new Error("transient");
        },
      },
      { deadLetterPublisher: publisher },
    ).handle(message());

    expect(outcome).toBe("nack");
    expect(published).toHaveLength(0);
  });

  test("nacks rather than acks when the dead-letter publish fails", async () => {
    // Losing a message is worse than redelivering it.
    const publisher = {
      publish: vi.fn(async () => {
        throw new Error("topic unavailable");
      }),
    };
    const outcome = await pipelineWith(
      {
        [TYPE]: () => {
          throw new TerminalEventError("unknown reference");
        },
      },
      { deadLetterPublisher: publisher },
    ).handle(message());

    expect(outcome).toBe("nack");
  });

  test("nacks when a terminal error is raised but no dead-letter topic is configured", async () => {
    const outcome = await pipelineWith({
      [TYPE]: () => {
        throw new TerminalEventError("unknown reference");
      },
    }).handle(message());

    expect(outcome).toBe("nack");
  });
});

describe("provenance", () => {
  test("records reason, stage, subscription and timestamp", async () => {
    const { publisher, published } = recordingPublisher();
    const now = new Date("2026-08-02T16:00:00.000Z");

    await pipelineWith(
      {
        [TYPE]: () => {
          throw new TerminalEventError("bad reference");
        },
      },
      { deadLetterPublisher: publisher, now: () => now },
    ).handle(message());

    expect(published[0]).toMatchObject({
      reason: expect.stringContaining("bad reference"),
      stage: "handler",
      subscription: SUBSCRIPTION,
      timestamp: now,
    });
  });

  test("passes the original message through untouched", async () => {
    const { publisher, published } = recordingPublisher();
    const original = message();

    await pipelineWith(
      {
        [TYPE]: () => {
          throw new TerminalEventError("bad reference");
        },
      },
      { deadLetterPublisher: publisher },
    ).handle(original);

    expect(published[0].message).toBe(original);
  });
});

describe("validation.onInvalidEnvelope", () => {
  const bad = () => message({ attributes: { "ce-specversion": "1.0" } });

  test("dead-letters by default", async () => {
    const { publisher, published } = recordingPublisher();
    const outcome = await pipelineWith({}, { deadLetterPublisher: publisher }).handle(bad());

    expect(outcome).toBe("dead-letter");
    expect(published[0].stage).toBe("envelope");
  });

  test("honours 'nack'", async () => {
    const { publisher } = recordingPublisher();
    const outcome = await pipelineWith(
      {},
      { deadLetterPublisher: publisher, validation: { onInvalidEnvelope: "nack" } },
    ).handle(bad());

    expect(outcome).toBe("nack");
  });

  test("honours 'ack'", async () => {
    const { publisher } = recordingPublisher();
    const outcome = await pipelineWith(
      {},
      { deadLetterPublisher: publisher, validation: { onInvalidEnvelope: "ack" } },
    ).handle(bad());

    expect(outcome).toBe("ack");
  });

  test("falls back to nack when dead-letter is configured but no publisher exists", async () => {
    expect(await pipelineWith({}).handle(bad())).toBe("nack");
  });
});

describe("validation.onDecodeFailure", () => {
  const bad = () => message({ data: Buffer.from("{not json") });

  test("dead-letters an unparseable body by default", async () => {
    const { publisher, published } = recordingPublisher();
    const outcome = await pipelineWith({ [TYPE]: () => {} }, { deadLetterPublisher: publisher }).handle(bad());

    expect(outcome).toBe("dead-letter");
    expect(published[0].stage).toBe("decode");
  });

  test("honours 'nack'", async () => {
    const { publisher } = recordingPublisher();
    const outcome = await pipelineWith(
      { [TYPE]: () => {} },
      { deadLetterPublisher: publisher, validation: { onDecodeFailure: "nack" } },
    ).handle(bad());

    expect(outcome).toBe("nack");
  });
});

describe("validation.requireDataschema", () => {
  test("is off by default", async () => {
    expect(await pipelineWith({ [TYPE]: () => {} }).handle(message())).toBe("ack");
  });

  test("rejects a message without ce-dataschema when required", async () => {
    const { publisher, published } = recordingPublisher();
    const outcome = await pipelineWith(
      { [TYPE]: () => {} },
      { deadLetterPublisher: publisher, validation: { requireDataschema: true } },
    ).handle(message());

    expect(outcome).toBe("dead-letter");
    expect(published[0].stage).toBe("envelope");
  });

  test("accepts a message carrying ce-dataschema when required", async () => {
    const withSchema = message({
      attributes: { ...message().attributes, "ce-dataschema": "https://schemas.example.test/thing/v1" },
    });
    const outcome = await pipelineWith({ [TYPE]: () => {} }, { validation: { requireDataschema: true } }).handle(
      withSchema,
    );

    expect(outcome).toBe("ack");
  });
});

describe("onUnhandledPattern", () => {
  test("acks by default", async () => {
    expect(await pipelineWith({}).handle(message())).toBe("ack");
  });

  test("honours 'nack'", async () => {
    expect(await pipelineWith({}, { onUnhandledPattern: "nack" }).handle(message())).toBe("nack");
  });

  test("honours 'dead-letter' with the unhandled stage", async () => {
    const { publisher, published } = recordingPublisher();
    const outcome = await pipelineWith(
      {},
      { deadLetterPublisher: publisher, onUnhandledPattern: "dead-letter" },
    ).handle(message());

    expect(outcome).toBe("dead-letter");
    expect(published[0].stage).toBe("unhandled");
  });
});
