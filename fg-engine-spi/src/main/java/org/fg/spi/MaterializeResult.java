package org.fg.spi;

import java.time.Duration;
import org.apache.arrow.vector.types.pojo.Schema;

/** 终态权威事实由 manifest 承载；此结果为 attach 流结束时的快速回报。 */
public record MaterializeResult(
    Schema schema, long rowCount, long bytesWritten, Duration executionTime) {}
