package org.fg.spark.sink;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;

/**
 * FG patch (D34)：写端前缀加固工厂——上游 apache/arrow-java#1116（12.0.1～19.0.0 受影响，
 * 20.0.0 于 #1119 修复）的 sink 侧移植。
 *
 * <p><b>上游 bug</b>：{@code AbstractCompressionCodec.compress} 在空检查、大小比较、写前缀
 * 三处分别读取 {@code writerIndex()}——特定时序下 8 字节未压缩长度前缀被写成 0（或漂移值），
 * 而 zstd/lz4 frame 本身完好。读端见前缀 0 走 empty shortcut → loadFieldBuffers 抛
 * IndexOutOfBoundsException → VectorLoader catch 漏 ownBuffers → 子分配器泄漏告警掩盖
 * 原始异常（2026-10-13 网关实测复现）。
 *
 * <p><b>本工厂</b>：包一层 codec——进 {@link #compress} 先捕获一次 {@code writerIndex()}
 * （#1119 同义），内层压缩后校验产出前缀：非空源却见前缀 0/漂移 → 用捕获值改写；非空源
 * 只产出 8 字节（空 shortcut 形态，frame 已丢）→ 无法恢复，抛错宁写败勿静默截断。decompress
 * 透传（sink 不解压）。
 */
final class FgPrefixHardenedFactory implements CompressionCodec.Factory {

  private final CompressionCodec.Factory delegate;

  private FgPrefixHardenedFactory(CompressionCodec.Factory delegate) {
    this.delegate = delegate;
  }

  static CompressionCodec.Factory wrap(CompressionCodec.Factory delegate) {
    return new FgPrefixHardenedFactory(delegate);
  }

  @Override
  public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
    return new HardenedCodec(delegate.createCodec(codecType));
  }
  // 注：arrow 12.0.1 的 Factory 接口无两参 createCodec(type, level)——sink 编译目标即 12.0.1

  private static final class HardenedCodec implements CompressionCodec {

    private final CompressionCodec inner;

    HardenedCodec(CompressionCodec inner) {
      this.inner = inner;
    }

    @Override
    public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
      // 上游 #1119：单一事实源——进压缩前捕获一次长度（内层多次读取 writerIndex 是 bug 根因）
      long declared = uncompressedBuffer.writerIndex();
      ArrowBuf compressed = inner.compress(allocator, uncompressedBuffer); // 内层负责释放输入
      if (declared == 0) {
        return compressed; // 空源的 8 字节 shortcut（前缀 0）是合法形态
      }
      if (compressed.writerIndex() < CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH) {
        compressed.close();
        throw new IllegalStateException(
            "compression codec produced buffer shorter than prefix: " + inner.getCodecType());
      }
      if (compressed.writerIndex() == CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH) {
        // 非空源却只产 8 字节 = 空 shortcut 形态、frame 已丢（上游 bug 最恶形态）——宁写败
        compressed.close();
        throw new IllegalStateException(
            "compression codec dropped payload for non-empty buffer (declared=" + declared
                + "): " + inner.getCodecType());
      }
      long prefix = compressed.getLong(0);
      if (prefix != CompressionUtil.NO_COMPRESSION_LENGTH && prefix != declared) {
        // #1116：前缀 0/漂移 + 完好 frame → 改写为捕获值（数据无损，仅元数据修正）
        compressed.setLong(0, declared);
      }
      return compressed;
    }

    @Override
    public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
      return inner.decompress(allocator, compressedBuffer); // sink 只写不读
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
      return inner.getCodecType();
    }
  }
}
