package org.fg.spark.sink;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.compression.CompressionCodec;
import org.apache.arrow.vector.compression.CompressionUtil;
import org.apache.arrow.vector.compression.NoCompressionCodec;
import org.junit.jupiter.api.Test;

/**
 * D34 写端前缀加固单测：模拟上游 apache/arrow-java#1116 形态的内层 codec（前缀清零 /
 * 前缀漂移 / 非空源丢 payload），断言 FgPrefixHardenedFactory 的三种处置。
 */
class FgPrefixHardenedFactoryTest {

  private static final class StubCodec implements CompressionCodec {
    final long prefixToWrite;
    final int payloadBytes; // 产出 frame 字节数（前缀之后）

    StubCodec(long prefixToWrite, int payloadBytes) {
      this.prefixToWrite = prefixToWrite;
      this.payloadBytes = payloadBytes;
    }

    @Override
    public ArrowBuf compress(org.apache.arrow.memory.BufferAllocator a, ArrowBuf src) {
      ArrowBuf out = a.buffer(CompressionUtil.SIZE_OF_UNCOMPRESSED_LENGTH + payloadBytes);
      out.setLong(0, prefixToWrite);
      for (int i = 0; i < payloadBytes; i++) {
        out.setByte(8 + i, i & 0xFF);
      }
      out.writerIndex(8 + payloadBytes);
      src.close(); // 内层契约：释放输入
      return out;
    }

    @Override
    public ArrowBuf decompress(org.apache.arrow.memory.BufferAllocator a, ArrowBuf compressed) {
      throw new UnsupportedOperationException();
    }

    @Override
    public CompressionUtil.CodecType getCodecType() {
      return CompressionUtil.CodecType.ZSTD;
    }
  }

  @Test
  void zeroPrefixRewrittenToCapturedLength() {
    CompressionCodec hardened = hardened(new StubCodec(0L, 100));
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
      ArrowBuf src = alloc.buffer(2000);
      src.writerIndex(2000);
      try (ArrowBuf out = hardened.compress(alloc, src)) {
        assertThat(out.getLong(0)).isEqualTo(2000L); // 改写为捕获长度
        assertThat(out.writerIndex()).isEqualTo(8 + 100);
      }
    }
  }

  @Test
  void driftedPrefixRewritten() {
    CompressionCodec hardened = hardened(new StubCodec(12345L, 100)); // 漂移前缀
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
      ArrowBuf src = alloc.buffer(2000);
      src.writerIndex(2000);
      try (ArrowBuf out = hardened.compress(alloc, src)) {
        assertThat(out.getLong(0)).isEqualTo(2000L);
      }
    }
  }

  @Test
  void validPrefixUntouched() {
    CompressionCodec hardened = hardened(new StubCodec(2000L, 100)); // 正确前缀
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
      ArrowBuf src = alloc.buffer(2000);
      src.writerIndex(2000);
      try (ArrowBuf out = hardened.compress(alloc, src)) {
        assertThat(out.getLong(0)).isEqualTo(2000L);
        assertThat(out.getByte(8)).isEqualTo((byte) 0);
      }
    }
  }

  @Test
  void emptySourcePrefixZeroIsLegal() {
    CompressionCodec hardened = hardened(new StubCodec(0L, 0)); // 空源 8 字节 shortcut
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
      ArrowBuf src = alloc.buffer(0);
      try (ArrowBuf out = hardened.compress(alloc, src)) {
        assertThat(out.getLong(0)).isEqualTo(0L);
        assertThat(out.writerIndex()).isEqualTo(8L);
      }
    }
  }

  @Test
  void droppedPayloadFailsFast() {
    CompressionCodec hardened = hardened(new StubCodec(0L, 0)); // 非空源却只产 8 字节
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
      ArrowBuf src = alloc.buffer(2000);
      src.writerIndex(2000);
      assertThatThrownBy(() -> hardened.compress(alloc, src))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("dropped payload");
    }
  }

  @Test
  void rawPackagedPrefixMinusOneUntouched() {
    // raw 包装（-1 前缀 + 原文）不经过 zstd 前缀语义——不应被改写
    CompressionCodec hardened = hardened(
        new CompressionCodec() {
          @Override
          public ArrowBuf compress(org.apache.arrow.memory.BufferAllocator a, ArrowBuf src) {
            long n = src.writerIndex();
            ArrowBuf out = a.buffer(8 + n);
            out.setLong(0, CompressionUtil.NO_COMPRESSION_LENGTH);
            out.setBytes(8, src, 0, n);
            out.writerIndex(8 + n);
            src.close();
            return out;
          }

          @Override
          public ArrowBuf decompress(
              org.apache.arrow.memory.BufferAllocator a, ArrowBuf compressed) {
            throw new UnsupportedOperationException();
          }

          @Override
          public CompressionUtil.CodecType getCodecType() {
            return CompressionUtil.CodecType.ZSTD;
          }
        });
    try (RootAllocator alloc = new RootAllocator(Long.MAX_VALUE)) {
      ArrowBuf src = alloc.buffer(500);
      src.writerIndex(500);
      try (ArrowBuf out = hardened.compress(alloc, src)) {
        assertThat(out.getLong(0)).isEqualTo(CompressionUtil.NO_COMPRESSION_LENGTH);
      }
    }
  }

  private static CompressionCodec hardened(CompressionCodec stub) {
    return FgPrefixHardenedFactory.wrap(
            new CompressionCodec.Factory() {
              @Override
              public CompressionCodec createCodec(CompressionUtil.CodecType codecType) {
                return stub;
              }
            })
        .createCodec(CompressionUtil.CodecType.ZSTD);
  }
}
