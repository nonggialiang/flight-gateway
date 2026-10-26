-- D39：submit 确认标记。NULL = 引擎 submit RPC（executePlan）尚未被首响应确认
--   （operationId 回显未到）。poll/attach 对 NULL 行门控等待，不再抢跑 attach 后靠
--   "operation not found" 宽限重试兜底（D36 orphan-grace 的推断式判死改为标记式）。
-- 升级：停旧启新（同 V2）；存量 RUNNING 且 submitted_at IS NULL 的行由 sweeper 在
--   fg.query.timeout 兜底——其提交实例已消亡，本就不可恢复。
ALTER TABLE fg_operation ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ;
