/*
 * Ported from Dremio OSS (services context module com.dremio.context.RequestContext concept),
 * Apache License 2.0. Minimal immutable implementation for cross-thread-pool propagation.
 */
package org.fg.common.context;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/** Immutable request context carried across thread pools via ThreadLocal. */
public final class RequestContext implements AutoCloseable {

  private static final RequestContext EMPTY = new RequestContext(Map.of());
  private static final ThreadLocal<RequestContext> CURRENT = new ThreadLocal<>();

  private final Map<Key<?>, Object> values;

  private RequestContext(Map<Key<?>, Object> values) {
    this.values = values;
  }

  public static RequestContext empty() {
    return EMPTY;
  }

  /** The context associated with the current thread, or the empty context. */
  public static RequestContext current() {
    final RequestContext context = CURRENT.get();
    return context == null ? EMPTY : context;
  }

  public <T> T get(Key<T> key) {
    Object value = values.get(key);
    return key.valueType.cast(value);
  }

  public <T> RequestContext with(Key<T> key, T value) {
    Map<Key<?>, Object> copy = new HashMap<>(values);
    copy.put(key, value);
    return new RequestContext(copy);
  }

  /** Runs the runnable with this context installed (restores the previous context afterwards). */
  public void run(Runnable runnable) {
    RequestContext previous = CURRENT.get();
    CURRENT.set(this);
    try {
      runnable.run();
    } finally {
      if (previous == null) {
        CURRENT.remove();
      } else {
        CURRENT.set(previous);
      }
    }
  }

  @Override
  public void close() {
    CURRENT.remove();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RequestContext)) {
      return false;
    }
    return values.equals(((RequestContext) o).values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  /** Typed key for context values. */
  public static final class Key<T> {
    private final String name;
    private final Class<T> valueType;

    private Key(String name, Class<T> valueType) {
      this.name = name;
      this.valueType = valueType;
    }

    public static <T> Key<T> newKey(String name, Class<T> valueType) {
      return new Key<>(name, valueType);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Key)) {
        return false;
      }
      return name.equals(((Key<?>) o).name);
    }

    @Override
    public int hashCode() {
      return name.hashCode();
    }

    @Override
    public String toString() {
      return name;
    }
  }
}
