package org.fg.spark.client

import org.apache.spark.connect.proto.Plan
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

/** commandPlanRequest（D17）：Plan.root=SQL 关系，无 WriteOperation、无 ReattachOptions。 */
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
}
