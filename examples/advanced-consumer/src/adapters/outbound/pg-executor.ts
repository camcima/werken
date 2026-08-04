import type { Pool } from "pg";
import type { SqlExecutor } from "@werken/nestjs-google-pubsub";

/**
 * Mode A: a pool-backed executor. The dedup insert runs in its own transaction, separate from the
 * projection write, so a crash between the two still yields a duplicate — which is why the handler
 * stays idempotent regardless.
 *
 * `QueryResult.rowCount` is typed `number | null`, so the `?? 0` is load-bearing: the store reads
 * "rows returned" to decide whether a marker was newly written, and NaN would break both
 * directions silently.
 */
export function createPgExecutor(pool: Pool): SqlExecutor {
  return {
    execute: async (sql, params) => {
      const result = await pool.query(sql, params as unknown[]);
      return { rowCount: result.rowCount ?? 0 };
    },
  };
}
