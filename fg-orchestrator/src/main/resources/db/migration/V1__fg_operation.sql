-- fg_operation 作业表（design §4.2/D14/D17/D18/D19）：执行状态/operationId/mode/kind 落库，
-- 跨实例共享。量级 = 在途查询数（retention 清扫）。
CREATE TABLE fg_operation (
  query_id               UUID PRIMARY KEY,
  session_ref            TEXT NOT NULL,
  sql_hash               TEXT NOT NULL,
  user_name              TEXT NOT NULL,
  sql_text               TEXT NOT NULL,
  result_key_prefix      TEXT NOT NULL,
  -- D17 粗粒度 kind：QUERY（幂等注册+物化交付）| COMMAND（SET/SHOW/DESCRIBE/EXPLAIN/USE/DDL/DML，
  -- 结果内联 command_result、豁免指纹幂等）
  kind                   TEXT NOT NULL DEFAULT 'QUERY' CHECK (kind IN ('QUERY', 'COMMAND')),
  -- D18 命令结果（Arrow IPC stream bytes，含 schema message；COMMAND 终态内联交付）
  command_result         BYTEA,
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
  updated_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 首 poll 幂等（D19）：唯一约束只作用于 kind='QUERY'——副作用语句（DML/DDL/SET/RESET/USE）
-- 豁免指纹去重，每次执行都是新行（网络重试与幂等冲突时以"可重复执行"为先；只读命令
-- 同例保持一致，避免 SHOW/DESCRIBE 命中陈旧行）。
CREATE UNIQUE INDEX uq_fg_operation_query_fingerprint
  ON fg_operation (session_ref, sql_hash) WHERE kind = 'QUERY';

-- 在途命令幂等（D19 修正）：PollFlightInfo 每次调用都会 register（无 handle 可辨"同一
-- 次执行"），COMMAND 若完全无约束会逐 poll 重复执行（慢 DML 每 poll 一次 INSERT）。
-- 唯一约束只作用于 RUNNING——同 (session, sql) 同时至多一个在途命令：poll 循环期间
-- register 幂等复用在途行；行终态后索引即释放，下一次执行（显式重发/新 poll 循环）照常
-- 新行（D19 豁免保留）。已知限制：命令恰在两次 poll 之间终态时，下一 poll 会重执行一次
-- （边界竞争，非幂等 DML 场景以"至多一次在途"为限的权衡）。
CREATE UNIQUE INDEX uq_fg_operation_command_inflight
  ON fg_operation (session_ref, sql_hash) WHERE kind = 'COMMAND' AND status = 'RUNNING';

CREATE INDEX idx_fg_operation_status ON fg_operation (status);
CREATE INDEX idx_fg_operation_terminal ON fg_operation (terminal_at);
CREATE INDEX idx_fg_operation_created ON fg_operation (created_at);

-- fg_session_option 会话选项表（design D20）：Flight SetSessionOptions 通道的持久层——
-- SET 即时命令之外，选项经此表持久，openSession 时注入 GatewaySession.options、Spark kit
-- 按差异重放到 Connect session。
CREATE TABLE fg_session_option (
  session_ref  TEXT NOT NULL,
  key          TEXT NOT NULL,
  value        TEXT NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (session_ref, key)
);
