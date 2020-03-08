package io.eventuate.messaging.kafka.basic.consumer;

import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessage;
import io.eventuate.messaging.kafka.common.EventuateKafkaMultiMessageConverter;
import io.eventuate.messaging.kafka.consumer.KafkaMessage;
import io.eventuate.messaging.kafka.consumer.KafkaMessageHandler;
import io.eventuate.messaging.kafka.consumer.KafkaSubscription;
import io.eventuate.messaging.kafka.consumer.MessageConsumerKafkaImpl;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.util.test.async.Eventually;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

public abstract class AbstractEventuateKafkaBasicConsumerTest {

  protected abstract EventuateKafkaConfigurationProperties getKafkaProperties();
  protected abstract EventuateKafkaConsumerConfigurationProperties getConsumerProperties();
  protected abstract EventuateKafkaProducer getProducer();
  protected abstract MessageConsumerKafkaImpl getConsumer();
  private KafkaMessageHandler handler;

  public void shouldStopWhenHandlerThrowsException() {
    String subscriberId = "subscriber-" + System.currentTimeMillis();
    String topic = "topic-" + System.currentTimeMillis();

    EventuateKafkaConsumerMessageHandler handler = makeExceptionThrowingHandler();

    EventuateKafkaConsumer consumer = makeConsumer(subscriberId, topic, handler);

    sendMessages(topic);

    assertConsumerStopped(consumer);

    assertHandlerInvokedAtLeastOnce(handler);
  }

  public void shouldConsumeMessages() {
    String subscriberId = "subscriber-" + System.currentTimeMillis();
    String topic = "topic-" + System.currentTimeMillis();

    sendMessages(topic);

    handler = mock(KafkaMessageHandler.class);

    KafkaSubscription subscription = getConsumer().subscribe(subscriberId, Collections.singleton(topic), handler);

    Eventually.eventually(() -> {
      verify(handler, atLeastOnce()).accept(any());
    });

    subscription.close();

  }

  public void shouldConsumeMessagesWithBackPressure() {
    String subscriberId = "subscriber-" + System.currentTimeMillis();
    String topic = "topic-" + System.currentTimeMillis();
    LinkedBlockingQueue<KafkaMessage> messages = new LinkedBlockingQueue<>();

    for (int i = 0 ; i < 100; i++)
      sendMessages(topic);

    handler = kafkaMessage -> {
      try {
        TimeUnit.MILLISECONDS.sleep(20);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      messages.add(kafkaMessage);
    };

    KafkaSubscription subscription = getConsumer().subscribe(subscriberId, Collections.singleton(topic), handler);

    Eventually.eventually(() -> {
      assertEquals(200, messages.size());
    });

    subscription.close();

  }

  public void shouldConsumeBatchOfMessage() {
    String subscriberId = "subscriber-" + System.currentTimeMillis();
    String topic = "topic-" + System.currentTimeMillis();

    List<EventuateKafkaMultiMessage> messages = Arrays.asList(new EventuateKafkaMultiMessage(null, "a"),
            new EventuateKafkaMultiMessage(null, "b"), new EventuateKafkaMultiMessage(null, "c"));

    getProducer().send(topic, null, new EventuateKafkaMultiMessageConverter().convertMessagesToBytes(messages));

    handler = mock(KafkaMessageHandler.class);

    KafkaSubscription subscription = getConsumer().subscribe(subscriberId, Collections.singleton(topic), handler);

    Eventually.eventually(() -> {
      verify(handler, times(3)).accept(any());
    });

    subscription.close();
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
    verify(handler, atLeast(1)).apply(any(), any());
  }

  private EventuateKafkaConsumerMessageHandler makeExceptionThrowingHandler() {
    EventuateKafkaConsumerMessageHandler handler = mock(EventuateKafkaConsumerMessageHandler.class);

    doAnswer(invocation -> {
      CompletableFuture.runAsync(() -> ((BiConsumer<Void, Throwable>)invocation.getArguments()[1]).accept(null, new RuntimeException("Test is simulating failure")));
      return null;
    }).when(handler).apply(any(), any());
    return handler;
  }

  private void assertConsumerStopped(EventuateKafkaConsumer consumer) {
    Eventually.eventually(() -> {
      assertEquals(EventuateKafkaConsumerState.MESSAGE_HANDLING_FAILED, consumer.getState());
    });
  }

}