package org.fg.frontend;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.fg.result.manifest.BatchIndex;
import org.junit.jupiter.api.Test;

/** D27 页规划纯函数：part/batch 两级相交收敛的映射表。 */
class ScrollPagePlannerTest {

  private static BatchIndex bidx(long... rows) {
    StringBuilder sb = new StringBuilder("{\"batches\":[");
    long offset = 100;
    for (int i = 0; i < rows.length; i++) {
      if (i > 0) {
        sb.append(',');
      }
      sb.append('[').append(offset).append(',').append(rows[i] * 10).append(',').append(rows[i]).append(']');
      offset += rows[i] * 10;
    }
    sb.append("]}");
    return BatchIndex.parse(sb.toString().getBytes());
  }

  @Test
  void partPlanningMiddleRangeSkipsEarlierParts() {
    // parts 行数 [100, 200, 50]：offset=150 limit=50 → 只碰 part1 的 [50,100)
    List<ScrollPagePlanner.PartSlice> slices =
        ScrollPagePlanner.planParts(new long[] {100, 200, 50}, 150, 50);
    assertThat(slices).containsExactly(new ScrollPagePlanner.PartSlice(1, 50, 50));
  }

  @Test
  void partPlanningSpanningRange() {
    // offset=80 limit=40 → part0 [80,100) 20 行 + part1 [0,20) 20 行
    List<ScrollPagePlanner.PartSlice> slices =
        ScrollPagePlanner.planParts(new long[] {100, 200, 50}, 80, 40);
    assertThat(slices).containsExactly(
        new ScrollPagePlanner.PartSlice(0, 80, 20),
        new ScrollPagePlanner.PartSlice(1, 0, 20));
  }

  @Test
  void partPlanningBeyondTotalIsEmpty() {
    assertThat(ScrollPagePlanner.planParts(new long[] {100, 200}, 300, 10)).isEmpty();
    assertThat(ScrollPagePlanner.planParts(new long[] {100, 200}, 1000, 10)).isEmpty();
  }

  @Test
  void partPlanningTailClampedByTotal() {
    // limit 超总行数：尾段自然截断
    assertThat(ScrollPagePlanner.planParts(new long[] {100, 200}, 250, 100))
        .containsExactly(new ScrollPagePlanner.PartSlice(1, 150, 50));
  }

  @Test
  void partPlanningZeroLimitIsEmpty() {
    assertThat(ScrollPagePlanner.planParts(new long[] {100}, 0, 0)).isEmpty();
  }

  @Test
  void batchPlanningSlicesWithinAndAcrossBatches() {
    // batch 行数 [256, 256, 100]：offset=200 limit=100 → b0 [200,256) 56 + b1 [0,44) 44
    List<ScrollPagePlanner.BatchSlice> slices =
        ScrollPagePlanner.planBatches(bidx(256, 256, 100), 200, 100);
    assertThat(slices).containsExactly(
        new ScrollPagePlanner.BatchSlice(0, 200, 56),
        new ScrollPagePlanner.BatchSlice(1, 0, 44));
  }

  @Test
  void batchPlanningSingleBatchPartial() {
    assertThat(ScrollPagePlanner.planBatches(bidx(500), 10, 5))
        .containsExactly(new ScrollPagePlanner.BatchSlice(0, 10, 5));
  }

  @Test
  void batchPlanningBeyondPartIsEmpty() {
    assertThat(ScrollPagePlanner.planBatches(bidx(500), 500, 10)).isEmpty();
  }

  @Test
  void batchPlanningCoversFullRange() {
    assertThat(ScrollPagePlanner.planBatches(bidx(256, 256), 0, 512)).containsExactly(
        new ScrollPagePlanner.BatchSlice(0, 0, 256),
        new ScrollPagePlanner.BatchSlice(1, 0, 256));
  }
}
