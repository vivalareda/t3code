import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";

/**
 * Durable child-agent transcript storage, scoped under the parent thread.
 *
 * Rows are append-only transcript chunks keyed by (thread, provider instance,
 * process run, child). `seq` is per-child and monotonically increasing so a
 * viewer can join historical rows to live events without a gap or duplicate.
 * Bulk child transcripts never enter the ordinary thread activity stream.
 */
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE IF NOT EXISTS projection_child_transcripts (
      thread_id TEXT NOT NULL,
      instance_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      child_id TEXT NOT NULL,
      seq INTEGER NOT NULL,
      chunk_json TEXT NOT NULL,
      created_at TEXT NOT NULL,
      PRIMARY KEY (thread_id, instance_id, run_id, child_id, seq)
    )
  `;

  // The primary key is exactly (thread_id, instance_id, run_id, child_id, seq),
  // so the read path (filter on the first four, order by seq) already uses it;
  // a secondary index over the same columns would only duplicate it.

  yield* sql`
    CREATE TABLE IF NOT EXISTS projection_child_states (
      thread_id TEXT NOT NULL,
      instance_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      child_id TEXT NOT NULL,
      title TEXT,
      backend TEXT,
      cwd TEXT,
      model TEXT,
      effort TEXT,
      status TEXT NOT NULL,
      outcome_status TEXT,
      summary TEXT,
      error_text TEXT,
      tokens INTEGER,
      context_window INTEGER,
      started_at TEXT,
      settled_at TEXT,
      updated_at TEXT NOT NULL,
      PRIMARY KEY (thread_id, instance_id, run_id, child_id)
    )
  `;

  yield* sql`
    CREATE INDEX IF NOT EXISTS idx_projection_child_states_thread
    ON projection_child_states (thread_id, instance_id, run_id)
  `;
});
