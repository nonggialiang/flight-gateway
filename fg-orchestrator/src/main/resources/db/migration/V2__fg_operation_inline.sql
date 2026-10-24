-- D38：inline 流式取数模式（不物化，消费 DoGet 经 ReattachExecute 增量取数）
--   mode 扩三值 RELAY/HTTPS/INLINE（线上四值 relay-forward|relay-scroll|https|inline 中
--   relay-* 两值由 mode=RELAY + scrollable 标志推导）
ALTER TABLE fg_operation DROP CONSTRAINT fg_operation_mode_check;
ALTER TABLE fg_operation ADD CONSTRAINT fg_operation_mode_check
  CHECK (mode IN ('HTTPS', 'RELAY', 'INLINE'));

-- D36 遗留清理：attach 租约两列（D36 起停止写入，终态清列子句已随 D38 移除）
-- 注意：不可逆——升级前快照；旧版本网关（仍写此列）在迁移后 INSERT 会报错，须先升网关
ALTER TABLE fg_operation DROP COLUMN IF EXISTS attach_owner;
ALTER TABLE fg_operation DROP COLUMN IF EXISTS attach_lease_until;
