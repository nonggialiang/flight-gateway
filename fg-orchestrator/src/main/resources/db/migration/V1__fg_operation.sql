-- fg_operation 作业表（design §4.2/D14）：执行状态/operationId/mode 落库，跨实例共享。
-- 量级 = 在途查询数（retention 清扫）。
CREATE TABLE fg_operation (
  query_id               UUID PRIMARY KEY,
  session_ref            TEXT NOT NULL,
  sql_hash               TEXT NOT NULL,
  user_name              TEXT NOT NULL,
  sql_text               TEXT NOT NULL,
  result_key_prefix      TEXT NOT NULL,
  mode                   TEXT NOT NULL CHECK (mode IN ('HTTPS', 'RELAY')),
  ordered                BOOLEAN NOT NULL DEFAULT FALSE,
  schema_bytes           BYTEA,
  status                 TEXT NOT NULL CHECK (status IN ('RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED')),
  connect_operation_id   TEXT,
  attach_owner           TEXT,
  attach_lease_until     TIMESTAMPTZ,
  engine_ref             TEXT,
  error                  TEXT,
  terminal_at            TIMESTAMPTZ,
  created_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now(),
  -- 首 poll 幂等：唯一键 = 会话 + SQL 指纹（网络重试不重复执行）
  CONSTRAINT fg_operation_fingerprint UNIQUE (session_ref, sql_hash)
);

CREATE INDEX idx_fg_operation_status ON fg_operation (status);
CREATE INDEX idx_fg_operation_terminal ON fg_operation (terminal_at);
CREATE INDEX idx_fg_operation_created ON fg_operation (created_at);
