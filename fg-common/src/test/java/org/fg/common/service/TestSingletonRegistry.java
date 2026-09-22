package org.fg.common.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Registry starts services in order and closes them in reverse order. */
class TestSingletonRegistry {

  @Test
  void startsInOrderClosesInReverse() throws Exception {
    SingletonRegistry registry = new SingletonRegistry();
    List<String> events = new ArrayList<>();

    registry.bind(
        MyServiceA.class,
        new MyServiceA() {
          @Override
          public void start() {
            events.add("start-a");
          }

          @Override
          public void close() {
            events.add("close-a");
          }
        });
    registry.bind(
        MyServiceB.class,
        new MyServiceB() {
          @Override
          public void start() {
            events.add("start-b");
          }

          @Override
          public void close() {
            events.add("close-b");
          }
        });

    registry.start();
    registry.close();

    assertThat(events).containsExactly("start-a", "start-b", "close-b", "close-a");
  }

  @Test
  void lookupAndDeferredProvider() throws Exception {
    SingletonRegistry registry = new SingletonRegistry();
    MyServiceA impl =
        new MyServiceA() {
          @Override
          public void start() {}

          @Override
          public void close() {}
        };
    registry.bind(MyServiceA.class, impl);
    registry.bind(
        MyCloseable.class,
        new MyCloseable() {
          @Override
          public void close() {}
        });

    assertThat(registry.lookup(MyServiceA.class)).isSameAs(impl);
    assertThat(registry.provider(MyServiceA.class).get()).isSameAs(impl);
    registry.close();
  }

  @Test
  void doubleStartRejected() throws Exception {
    SingletonRegistry registry = new SingletonRegistry();
    class Once implements Service {
      @Override
      public void start() {}

      @Override
      public void close() {}
    }
    registry.bindSelf(new Once());
    registry.start();
    // second start hits the wrapper's state guard (INIT -> STARTED already)
    assertThatThrownBy(registry::start).isInstanceOf(IllegalArgumentException.class);
    registry.close();
  }

  interface MyServiceA extends Service {}

  interface MyServiceB extends Service {}

  interface MyCloseable extends AutoCloseable {}
}
