import { describe, expect, test, vi } from "vitest";
import { createPgExecutor } from "../src/adapters/outbound/pg-executor.js";

/** `pg` types QueryResult.rowCount as `number | null`. Null must read as zero, not NaN. */
describe("pg executor", () => {
  test("reports the row count the driver returned", async () => {
    const pool = { query: vi.fn(async () => ({ rowCount: 1 })) };
    const executor = createPgExecutor(pool as never);

    expect(await executor.execute("SELECT 1", [])).toEqual({ rowCount: 1 });
  });

  test("reads a null row count as zero", async () => {
    const pool = { query: vi.fn(async () => ({ rowCount: null })) };
    const executor = createPgExecutor(pool as never);

    expect(await executor.execute("SELECT 1", [])).toEqual({ rowCount: 0 });
  });

  test("passes sql and params straight through", async () => {
    const pool = { query: vi.fn(async () => ({ rowCount: 0 })) };
    const executor = createPgExecutor(pool as never);

    await executor.execute("SELECT $1", ["x"]);

    expect(pool.query).toHaveBeenCalledWith("SELECT $1", ["x"]);
  });
});
