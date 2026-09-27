package org.fg.frontend;

import java.util.ArrayList;
import java.util.List;
import org.fg.result.manifest.BatchIndex;

/**
 * D27 页规划（纯函数，Dremio JobResultsStore.loadJobData 同构的两级收敛）：
 *
 * <ol>
 *   <li><b>part 级</b>：按 manifest 的 per-part recordCount 累计行数——整个落在页区间之前的
 *       part 直接跳过（不打开对象），只选与 [offset, offset+limit) 相交的 part 并折算
 *       part 内偏移/行数；</li>
 *   <li><b>batch 级</b>：选定 part 内按 .bidx 的 per-batch rows 同法收敛到相交 batch
 *       （无 bidx 时由调用方回落整 part 顺序读）。</li>
 * </ol>
 */
final class ScrollPagePlanner {

  private ScrollPagePlanner() {}

  /** part 切片：第 partIndex 个 part 内取 [offset, offset+rows) 行。 */
  record PartSlice(int partIndex, long offset, long rows) {}

  /** batch 切片：第 batchIndex 个 batch 内取 [start, start+rows) 行。 */
  record BatchSlice(int batchIndex, long start, long rows) {}

  /** part 级行规划（partRows = 各 part 行数，manifest 顺序）。 */
  static List<PartSlice> planParts(long[] partRows, long offset, long limit) {
    List<PartSlice> out = new ArrayList<>();
    long running = 0;
    long remaining = limit;
    for (int i = 0; i < partRows.length && remaining > 0; i++) {
      long rows = partRows[i];
      if (offset < running + rows) { // 相交
        long partOffset = Math.max(0, offset - running);
        long take = Math.min(rows - partOffset, remaining);
        if (take > 0) {
          out.add(new PartSlice(i, partOffset, take));
          remaining -= take;
        }
      }
      running += rows;
    }
    return out;
  }

  /** batch 级行规划（bidx 的 per-batch rows；offset/limit 已是 part 内折算值）。 */
  static List<BatchSlice> planBatches(BatchIndex index, long offset, long limit) {
    List<BatchSlice> out = new ArrayList<>();
    long running = 0;
    long remaining = limit;
    List<BatchIndex.Batch> batches = index.batches();
    for (int i = 0; i < batches.size() && remaining > 0; i++) {
      long rows = batches.get(i).rows();
      if (offset < running + rows) {
        long start = Math.max(0, offset - running);
        long take = Math.min(rows - start, remaining);
        if (take > 0) {
          out.add(new BatchSlice(i, start, take));
          remaining -= take;
        }
      }
      running += rows;
    }
    return out;
  }
}
