package org.fg.spark.client

import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.io.TempDir
import java.nio.file.Files
import java.nio.file.Path

/** D27 后补：launch.conf-file（spark-defaults.conf 风格 properties）装载。 */
class EngineLauncherConfFileTest {

  @TempDir
  var tmp: Path = _

  @Test
  def parsesCommentsSeparatorsAndContinuation(): Unit = {
    val f = tmp.resolve("base.conf")
    Files.writeString(f,
      """# 公共基础配置
        |! also comment
        |spark.sql.adaptive.enabled=true
        |spark.sql.shuffle.partitions:8
        |spark.driver.memory 2g
        |spark.driver.extraJavaOptions=-Xmx2g \
        | -XX:+UseG1GC
        |# 空行与空键
        |
        |""".stripMargin)
    val m = EngineLauncher.loadConfFile(f.toString)
    assertEquals("true", m.get("spark.sql.adaptive.enabled"))
    assertEquals("8", m.get("spark.sql.shuffle.partitions"))
    assertEquals("2g", m.get("spark.driver.memory"))
    // 行续合并（Properties 语义：续行以单个空格连接）
    assertEquals("-Xmx2g -XX:+UseG1GC", m.get("spark.driver.extraJavaOptions"))
    assertEquals(4, m.size())
  }

  @Test
  def emptyOrMissingPathHandled(): Unit = {
    assertTrue(EngineLauncher.loadConfFile("").isEmpty)
    assertTrue(EngineLauncher.loadConfFile(null).isEmpty)
    assertThrows(classOf[IllegalStateException], () =>
      EngineLauncher.loadConfFile(tmp.resolve("nope.conf").toString))
  }

  @Test
  def tildeExpanded(): Unit = {
    val home = System.getProperty("user.home")
    val rel = "~/.fg-launch-conf-file-test.conf"
    val abs = home + "/.fg-launch-conf-file-test.conf"
    val created = !Files.exists(Path.of(abs))
    try {
      Files.writeString(Path.of(abs), "spark.k=v\n")
      val m = EngineLauncher.loadConfFile(rel)
      assertEquals("v", m.get("spark.k"))
    } finally {
      if (created) Files.deleteIfExists(Path.of(abs))
    }
  }
}
