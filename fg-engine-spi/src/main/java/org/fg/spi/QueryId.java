package org.fg.spi;

import java.util.UUID;

/** Gateway-side query identifier. Used as trace id across gateway and engine. */
public record QueryId(String value) {

  public QueryId {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("QueryId value must be non-blank");
    }
  }

  public static QueryId newId() {
    return new QueryId(UUID.randomUUID().toString());
  }

  @Override
  public String toString() {
    return value;
  }
}
