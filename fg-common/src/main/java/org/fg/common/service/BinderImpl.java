/*
 * Ported from Dremio OSS (common/legacy com.dremio.service.BinderImpl), Apache License 2.0.
 * Trimmed: no Guice, no Bindings iterator / BindingCreator / InjectableReference.
 */
package org.fg.common.service;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import java.util.Objects;
import javax.inject.Provider;

/**
 * A very basic injection provider implementation: bind singletons or providers to interfaces,
 * with deferred lookup to break circular startup dependencies.
 */
public class BinderImpl {

  private final BinderImpl parent;
  private volatile Map<Class<?>, Resolver> lookups = ImmutableMap.of();

  public BinderImpl() {
    this.parent = null;
  }

  public BinderImpl(BinderImpl parent) {
    this.parent = parent;
  }

  public void copyBindings(BinderImpl target) {
    synchronized (target) {
      ImmutableMap.Builder<Class<?>, Resolver> newMap = ImmutableMap.builder();
      newMap.putAll(target.lookups);
      newMap.putAll(lookups);
      target.lookups = newMap.build();
    }
  }

  public <T> Provider<T> provider(final Class<? extends T> iface) {
    return new DeferredProvider<>(iface);
  }

  class DeferredProvider<T> implements Provider<T> {
    private final Class<? extends T> iface;

    DeferredProvider(Class<? extends T> iface) {
      this.iface = iface;
    }

    @Override
    public T get() {
      return lookup(iface);
    }
  }

  Resolver getResolver(Class<?> iface) {
    Resolver r = lookups.get(iface);
    if (r != null) {
      return r;
    }
    if (parent != null) {
      return parent.getResolver(iface);
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  public <T> T lookup(Class<T> iface) {
    Preconditions.checkNotNull(iface, "Must provide desired interface or class.");
    Resolver resolver = getResolver(iface);
    Preconditions.checkNotNull(resolver, "Unable to find injectable based on %s", iface.getName());
    return (T) resolver.get(this);
  }

  /** Bind a singleton instance. */
  @SuppressWarnings("unchecked")
  public <IFACE> IFACE bindSelf(IFACE impl) {
    return bindInternal((Class<IFACE>) impl.getClass(), impl);
  }

  /** Bind a singleton instance. */
  public <IFACE> IFACE bind(Class<IFACE> iface, IFACE impl) {
    bind(iface, new GenericReference(impl));
    return impl;
  }

  /** Bind a user provided factory. */
  public synchronized <IFACE> void bindProvider(
      Class<IFACE> iface, Provider<? extends IFACE> provider) {
    Preconditions.checkNotNull(iface);
    final ImmutableMap.Builder<Class<?>, Resolver> newLookups = ImmutableMap.builder();
    newLookups.putAll(lookups);
    newLookups.put(iface, wrap(provider));
    lookups = newLookups.build();
  }

  public synchronized <IFACE> boolean bindIfUnbound(Class<IFACE> iface, IFACE impl) {
    if (!lookups.containsKey(iface)) {
      bind(iface, impl);
      return true;
    }
    return false;
  }

  private <IFACE> IFACE bindInternal(Class<IFACE> iface, IFACE impl) {
    bind(iface, wrap(impl));
    return impl;
  }

  protected synchronized <IFACE> void bind(Class<IFACE> iface, Resolver reference) {
    Preconditions.checkNotNull(iface);
    final ImmutableMap.Builder<Class<?>, Resolver> newLookups = ImmutableMap.builder();
    newLookups.putAll(lookups);
    newLookups.put(iface, reference);
    lookups = newLookups.build();
  }

  synchronized void remove(final Class<?> iface) {
    if (!lookups.containsKey(iface)) {
      throw new IllegalStateException(
          "Trying to remove an unbound value. No singleton is bound to " + iface.getName());
    }
    final ImmutableMap.Builder<Class<?>, Resolver> newLookups = ImmutableMap.builder();
    lookups.forEach((k, v) -> { if (!k.equals(iface)) newLookups.put(k, v); });
    lookups = newLookups.build();
  }

  public synchronized <IFACE> void replace(Class<IFACE> iface, IFACE impl) {
    remove(iface);
    bindInternal(iface, impl);
  }

  protected synchronized <IFACE> void replace(Class<IFACE> iface, Resolver reference) {
    remove(iface);
    bind(iface, reference);
  }

  public BinderImpl newChild() {
    return new BinderImpl(this);
  }

  static Resolver wrap(Object obj) {
    if (obj instanceof Resolver) {
      return (Resolver) obj;
    } else if (obj instanceof Provider) {
      return new ProviderReference((Provider<?>) obj);
    } else {
      return new GenericReference(obj);
    }
  }

  public interface Resolver {
    Object get(BinderImpl provider);
  }

  public static class GenericReference implements Resolver {
    private final Object inner;

    public GenericReference(Object inner) {
      this.inner = inner;
    }

    @Override
    public Object get(BinderImpl provider) {
      return inner;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return Objects.equals(inner, ((GenericReference) o).inner);
    }

    @Override
    public int hashCode() {
      return Objects.hash(inner);
    }
  }

  public static class ProviderReference implements Resolver {
    private final Provider<?> provider;

    public ProviderReference(Provider<?> provider) {
      this.provider = provider;
    }

    @Override
    public Object get(BinderImpl binder) {
      return provider.get();
    }

    public Provider<?> getProvider() {
      return provider;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      return Objects.equals(provider, ((ProviderReference) o).provider);
    }

    @Override
    public int hashCode() {
      return Objects.hash(provider);
    }
  }
}
