package io.eventuate.messaging.kafka.basic.consumer;

import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.util.test.async.Eventually;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public abstract class AbstractEventuateKafkaBasicConsumerTest {

  protected abstract EventuateKafkaConfigurationProperties getKafkaProperties();
  protected abstract EventuateKafkaConsumerConfigurationProperties getConsumerProperties();
  protected abstract EventuateKafkaProducer getProducer();

  public void shouldStopWhenHandlerThrowsException() {
    String subscriberId = "subscriber-" + System.currentTimeMillis();
    String topic = "topic-" + System.currentTimeMillis();

    EventuateKafkaConsumerMessageHandler handler = makeExceptionThrowingHandler();

    EventuateKafkaConsumer consumer = makeConsumer(subscriberId, topic, handler);

    sendMessages(topic);

    assertConsumerStopped(consumer);

    assertHandlerInvokedAtLeastOnce(handler);
  }

  private EventuateKafkaConsumer makeConsumer(String subscriberId, String topic, EventuateKafkaConsumerMessageHandler handler) {
    EventuateKafkaConsumer consumer = new EventuateKafkaConsumer(subscriberId,
            handler,
            Collections.singletonList(topic),
            getKafkaProperties().getBootstrapServers(),
            getConsumerProperties());

    consumer.start();
    return consumer;
  }

  private void sendMessages(String topic) {
    getProducer().send(topic, "1", "a");
    getProducer().send(topic, "1", "b");
  }

  private void assertHandlerInvokedAtLeastOnce(EventuateKafkaConsumerMessageHandler handler) {
    verify(handler, atLeast(1)).accept(any(), any());
  }

  private EventuateKafkaConsumerMessageHandler makeExceptionThrowingHandler() {
    EventuateKafkaConsumerMessageHandler handler = mock(EventuateKafkaConsumerMessageHandler.class);

    doAnswer(invocation -> {
      CompletableFuture.runAsync(() -> ((BiConsumer<Void, Throwable>)invocation.getArguments()[1]).accept(null, new RuntimeException()));
      return null;
    }).when(handler).accept(any(), any());
    return handler;
  }

  private void assertConsumerStopped(EventuateKafkaConsumer consumer) {
    Eventually.eventually(() -> {
      assertEquals(EventuateKafkaConsumerState.MESSAGE_HANDLING_FAILED, consumer.getState());
    });
  }

}