-- fg_operation 作业表（design §4.2/D14/D17/D18/D19）：执行状态/operationId/mode/kind 落库，
-- 跨实例共享。量级 = 在途查询数（retention 清扫）。
CREATE TABLE fg_operation (
  -- D28 续传协议：queryId 客户端可铸造（x-fg-query-id 头，不透明串 ≤128）或服务端
  -- 生成（无头路径）——TEXT 主键
  query_id               TEXT PRIMARY KEY,
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
  -- D27 scroll 随机翻页：注册时客户端声明 TYPE_SCROLL_INSENSITIVE（头 x-fg-result-set-type:
  -- scroll）即落 true——mode 同时强制 RELAY，终态 FlightInfo 恒单 STREAM endpoint，页经
  -- DoGet + x-fg-page-offset/limit 头切片（无头 DoGet = 全量顺序流，第三方零惊讶）
  scrollable             BOOLEAN NOT NULL DEFAULT FALSE,
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

-- 首 poll 幂等（D19，D28 修订）：唯一约束收窄到 RUNNING——poll 循环期间（行在途）
-- 同指纹重复注册收敛到一行；行终态后索引释放，同指纹允许新行（x-fg-query-id 显式
-- 续传/新执行；无头路径的终态复用由 register 的 getByFingerprint 查询实现，不经此索引）。
CREATE UNIQUE INDEX uq_fg_operation_query_fingerprint
  ON fg_operation (session_ref, sql_hash) WHERE kind = 'QUERY' AND status = 'RUNNING';

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

-- fg_session 会话登记表（design D20 生命周期绑定）：fg 会话 ↔ 引擎 Connect 会话一一对应，
-- 且 session_ref 即 Connect session id（客户端自报 UUID 直接透传，零映射；入口强制 UUID
-- 格式——Connect INVALID_HANDLE.FORMAT 约束前移）。生命周期：
--   born（首次接触 x-fg-session-id 自报）→ ACTIVE
--   → CLOSED（client 显式关闭 | engine_lost：attach 校验发现引擎侧会话已死）
-- CLOSED 为终态且 sticky——不复活、不翻新；客户端必须轮换 x-fg-session-id。
-- 会话选项不落盘（值在引擎会话 conf，随会话生灭），本表仅登记 option_keys 供
-- GetSessionOptions 回读；engine_ref 记录会话归属引擎（admin 调用定向，M2 引擎注册表
-- 落地前经配置解析地址）。
CREATE TABLE fg_session (
  session_ref        TEXT PRIMARY KEY,
  user_name          TEXT NOT NULL,
  engine_ref         TEXT NOT NULL,
  engine_started_at  BIGINT,
  option_keys        TEXT[] NOT NULL DEFAULT '{}',
  status             TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'CLOSED')),
  closed_reason      TEXT,
  created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_active_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
  closed_at          TIMESTAMPTZ
);

CREATE INDEX idx_fg_session_status ON fg_session (status);
