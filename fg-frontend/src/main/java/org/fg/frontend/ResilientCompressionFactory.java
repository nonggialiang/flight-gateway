package org.fg.frontend;

import com.github.luben.zstd.Zstd;
import org.apache.arrow.compression.CommonsCompressionFactory;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * D34：读端容错压缩工厂（上游 apache/arrow-java#1116 的网关侧读恢复）。
 *
 * <p><b>背景</b>：arrow-java 写端（12.0.1～19.0.0 均受影响，20.0.0 修复）在
 * {@code AbstractCompressionCodec.compress} 多次读取 {@code writerIndex()}，特定时序下
 * zstd buffer 的 8 字节未压缩长度前缀可被写成 0，而 zstd frame 本身完好。读端
 * {@code AbstractCompressionCodec.decompress} 见前缀 0 → 走 empty shortcut 返回 0 字节
 * buffer → {@code loadFieldBuffers} 中途 IndexOutOfBoundsException → VectorLoader catch
 * 分支不关 ownBuffers → 子分配器 close 时报"Memory was leaked"并掩盖原始异常（实测
 * 2026-10-13，泄漏量 = 该字段已解压 buffer，如 16384 行 validity 恰为 2048B）。
 *
 * <p><b>本工厂</b>：仅拦"前缀 0 + 帧载荷非空"这一损坏形态——zstd frame 头自带
 * decompressed content size（写端 {@code Zstd.compressUnsafe} 恒写该字段），据其兜底
 * 解压；其余全部透传内层 codec（语义零变更）。LZ4 frame（sink FgLz4FrameCodec FLG=0x60）
 * 不携带 content size，无法读端兜底——写端加固见 fg-spark-sink。
 *
 * <p>契约对齐：{@link CompressionCodec#decompress} 的实现须负责释放压缩输入 buffer——
 * 兜底路径接管时同样关闭输入；兜底失败时透传内层（由内层按原语义处理/抛错）。
 */
final class ResilientCompressionFactory implements CompressionCodec.Factory {

  private static final Logger logger = LoggerFactory.getLogger(ResilientCompressionFactory.class);

  static final ResilientCompressionFactory INSTANCE =
      new ResilientCompressionFactory(CommonsCompressionFactory.INSTANCE);

  private final CompressionCodec.Factory delegate;

  private ResilientCompressionFactory(CompressionCodec.Factory delegate) {
    this.delegate = delegate;
  }

  @Override
  public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
    return new ResilientCodec(delegate.createCodec(codecType));
  }

  @Override
  public CompressionCodec createCodec(CompressionUtil.CodecType codecType, int compressionLevel) {
    return new ResilientCodec(delegate.createCodec(codecType, compressionLevel));
  }

  private static final class ResilientCodec implements CompressionCodec {

    private final CompressionCodec inner;

    ResilientCodec(CompressionCodec inner) {
      this.inner = inner;
    }

    @Override
    public ArrowBuf compress(BufferAllocator allocator, ArrowBuf uncompressedBuffer) {
      return inner.compress(allocator, uncompressedBuffer); // 读端不解压自产数据，直接透传
    }

    @Override
    public ArrowBuf decompress(BufferAllocator allocator, ArrowBuf compressedBuffer) {
      if (isZeroPrefixedZstd(compressedBuffer)) {
        try {
          return zstdFrameFallback(allocator, compressedBuffer);
        } catch (Exception e) {
          // 兜底不适用（非 zstd frame/无 content size/解压不一致）→ 内层按原语义处理
          logger.debug("zstd frame-size fallback not applicable: {}", e.toString());
        }
      }
      return inner.decompress(allocator, compressedBuffer);
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
      return inner.getCodecType();
    }

    /** #1116 形态：前缀 0 但帧载荷非空（合法空 buffer 是恰好 8 字节、前缀 0）。 */
    private static boolean isZeroPrefixedZstd(ArrowBuf compressed) {
      return compressed.writerIndex() > CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH
          && compressed.getLong(0) == 0L;
    }

    /** 用 zstd frame 头声明的 content size 兜底解压；成功则接管输入生命周期。 */
    private static ArrowBuf zstdFrameFallback(BufferAllocator allocator, ArrowBuf compressed) {
      long wi = compressed.writerIndex();
      byte[] body = new byte[(int) (wi - CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH)];
      compressed.getBytes(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH, body, 0, body.length);

      long contentSize = Zstd.getFrameContentSize(body, 0);
      if (contentSize <= 0 || contentSize > Integer.MAX_VALUE - 8) {
        throw new IllegalArgumentException("zstd frame content size unavailable: " + contentSize);
      }
      byte[] out = new byte[(int) contentSize];
      long n = Zstd.decompress(out, body); // 数组重载（ByteBuffer 重载要求 direct buffer）
      if (Zstd.isError(n)) {
        throw new IllegalArgumentException(
            "zstd decompress failed: " + Zstd.getErrorName(n));
      }
      if (n != contentSize) {
        throw new IllegalArgumentException(
            "zstd frame content size mismatch: declared=" + contentSize + " actual=" + n);
      }
      ArrowBuf buf = allocator.buffer(contentSize);
      buf.setBytes(0, out, 0, (int) contentSize);
      buf.writerIndex(contentSize);
      compressed.close(); // decompress 契约：接管输入须释放
      logger.warn(
          "Recovered #1116-corrupted zstd buffer via frame content size: declaredPrefix=0 "
              + "frameContentSize={} compressedBytes={}",
          contentSize, body.length);
      return buf;
    }
  }
}
