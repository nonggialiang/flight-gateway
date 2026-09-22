/*
 * Ported from Dremio OSS (common/legacy com.dremio.common.AutoCloseables), Apache License 2.0.
 */
package org.fg.common.util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Utilities for AutoCloseable classes. */
public final class AutoCloseables {
  private AutoCloseables() {}

  public static AutoCloseable all(final Iterable<? extends AutoCloseable> autoCloseables) {
    return () -> close(autoCloseables);
  }

  public static void close(Throwable t, AutoCloseable... autoCloseables) {
    close(t, Arrays.asList(autoCloseables));
  }

  public static void close(Throwable t, Iterable<? extends AutoCloseable> autoCloseables) {
    try {
      close(autoCloseables);
    } catch (Exception e) {
      t.addSuppressed(e);
    }
  }

  public static void close(AutoCloseable... autoCloseables) throws Exception {
    close(Arrays.asList(autoCloseables));
  }

  /** Closes all autoCloseables if not null, suppressing subsequent exceptions if more than one. */
  public static void close(Iterable<? extends AutoCloseable> ac) throws Exception {
    if (ac == null) {
      return;
    }
    if (ac instanceof AutoCloseable) {
      ((AutoCloseable) ac).close();
      return;
    }
    Exception topLevelException = null;
    for (AutoCloseable closeable : ac) {
      try {
        if (closeable != null) {
          closeable.close();
        }
      } catch (Exception e) {
        if (topLevelException == null) {
          topLevelException = e;
        } else if (e != topLevelException) {
          topLevelException.addSuppressed(e);
        }
      }
    }
    if (topLevelException != null) {
      throw topLevelException;
    }
  }

  public static Iterable<AutoCloseable> iter(AutoCloseable... ac) {
    if (ac.length == 0) {
      return Collections.emptyList();
    }
    List<AutoCloseable> nonNull = new ArrayList<>();
    for (AutoCloseable c : ac) {
      if (c != null) {
        nonNull.add(c);
      }
    }
    return nonNull;
  }

  /** close() without a checked exception; wraps into RuntimeException. May be null. */
  public static void closeNoChecked(final AutoCloseable autoCloseable) {
    if (autoCloseable != null) {
      try {
        autoCloseable.close();
      } catch (final Exception e) {
        throw new RuntimeException("Exception while closing: " + e.getMessage(), e);
      }
    }
  }

  private static final AutoCloseable NOOP = () -> {};

  public static AutoCloseable noop() {
    return NOOP;
  }
}
