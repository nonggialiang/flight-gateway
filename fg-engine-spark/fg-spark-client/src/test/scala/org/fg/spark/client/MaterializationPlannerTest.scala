package org.fg.spark.client

import org.apache.spark.connect.proto.Plan
import org.fg.spi.MaterializationSpec

import scala.collection.JavaConverters._


import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

/** commandPlanRequest（D17）/ executePlanRequest（D36 预生成 operationId）。 */
class MaterializationPlannerTest {

  @Test
  def commandPlanRequestIsBareSqlRelation(): Unit = {
    val request = MaterializationPlanner.commandPlanRequest("alice", "s-1", "SET k = v")
    assertEquals("s-1", request.getSessionId)
    assertEquals("alice", request.getUserContext.getUserId)

    val plan: Plan = request.getPlan
    assertTrue(plan.hasRoot, "command plan must carry root relation")
    assertFalse(plan.hasCommand, "command plan must not carry WriteOperation/Command")
    assertTrue(plan.getRoot.hasSql, "root must be SQL relation")
    assertEquals("SET k = v", plan.getRoot.getSql.getQuery)

    assertTrue(
      request.getRequestOptionsList.isEmpty,
      "command plan must not request ReattachOptions (gateway holds stream to terminal)")
  }

  /** D36：executePlan 请求携带编排层预生成 operationId（客户端可指定，引擎 UUID 规范化作执行身份）。 */
  @Test
  def executePlanRequestCarriesPregeneratedOperationId(): Unit = {
    val spec = new MaterializationSpec("s3a://b/p/", "zstd", 16384, null)
    val opId = java.util.UUID.randomUUID().toString
    val request =
      MaterializationPlanner.executePlanRequest("alice", "s-1", "SELECT 1", spec, opId)

    assertTrue(request.hasOperationId, "executePlan must carry client operationId")
    assertEquals(opId, request.getOperationId)
    assertTrue(
      request.getRequestOptionsList.asScala.exists(
        (o: org.apache.spark.connect.proto.ExecutePlanRequest.RequestOption) =>
          o.hasReattachOptions && o.getReattachOptions.getReattachable),
      "executePlan must stay reattachable")
  }

  /** D38：inline 请求 = 裸 SQL Relation + 预生成 operationId + reattachable（无 WriteOperation）。 */
  @Test
  def inlinePlanRequestIsBareSqlRelationWithOperationId(): Unit = {
    val opId = java.util.UUID.randomUUID().toString
    val request = MaterializationPlanner.inlinePlanRequest("alice", "s-1", "SELECT 1", opId)

    assertEquals(opId, request.getOperationId)
    val plan: Plan = request.getPlan
    assertTrue(plan.hasRoot, "inline plan must be bare SQL relation")
    assertFalse(plan.hasCommand, "inline plan must not carry WriteOperation/Command")
    assertTrue(plan.getRoot.hasSql)
    assertTrue(
      request.getRequestOptionsList.asScala.exists(
        (o: org.apache.spark.connect.proto.ExecutePlanRequest.RequestOption) =>
          o.hasReattachOptions && o.getReattachOptions.getReattachable),
      "inline must be reattachable (result_complete 门控)")
  }
}
