package io.eventuate.messaging.kafka.basic.consumer;

import io.eventuate.messaging.kafka.common.EventuateBinaryMessageEncoding;
import io.eventuate.messaging.kafka.common.EventuateKafkaConfigurationProperties;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.util.test.async.Eventually;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EventuateKafkaBasicConsumerTest.EventuateKafkaConsumerTestConfiguration.class)
public class EventuateKafkaBasicConsumerTest {

  @Configuration
  @EnableAutoConfiguration
  @EnableConfigurationProperties({EventuateKafkaConsumerConfigurationProperties.class, EventuateKafkaProducerConfigurationProperties.class})
  public static class EventuateKafkaConsumerTestConfiguration {

    @Bean
    public EventuateKafkaConfigurationProperties eventuateKafkaConfigurationProperties() {
      return new EventuateKafkaConfigurationProperties();
    }

    @Bean
    public EventuateKafkaProducer producer(EventuateKafkaConfigurationProperties kafkaProperties, EventuateKafkaProducerConfigurationProperties producerProperties) {
      return new EventuateKafkaProducer(kafkaProperties.getBootstrapServers(), producerProperties);
    }

  }

  @Autowired
  private EventuateKafkaConfigurationProperties kafkaProperties;

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties consumerProperties;

  @Autowired
  private EventuateKafkaProducer producer;

  String subscriberId;
  String topic;

  @Before
  public void init() {
    subscriberId = "subscriber-" + System.currentTimeMillis();
    topic = "topic-" + System.currentTimeMillis();
  }

  @Test
  public void testSendingToSpecificPartition() throws InterruptedException {
    int partitions = 2;

    BlockingQueue<ConsumerRecord<String, byte[]>> queue = new LinkedBlockingQueue();

    EventuateKafkaConsumerMessageHandler handler = (consumerRecord, voidThrowableBiConsumer) -> {
      queue.add(consumerRecord);
      return null;
    };

    EventuateKafkaConsumer consumer = makeConsumer(subscriberId, topic, handler);

    for (int i = 0; i < partitions; i++) {
      producer.send(topic, i, "key", EventuateBinaryMessageEncoding.stringToBytes(String.valueOf(i)));
    }

    Map<Integer, String> partitionValues = new HashMap<>();

    for (int i = 0; i < partitions; i++) {
      ConsumerRecord<String, byte[]> record = queue.poll(20, TimeUnit.SECONDS);
      Assert.assertNotNull(record);
      partitionValues.put(record.partition(), EventuateBinaryMessageEncoding.bytesToString(record.value()));
    }

    for (int i = 0; i < partitions; i++) {
      Assert.assertTrue(partitionValues.containsKey(i));
      Assert.assertEquals(String.valueOf(i), partitionValues.get(i));
    }
  }

  @Test
  public void shouldStopWhenHandlerThrowsException() {
    EventuateKafkaConsumerMessageHandler handler = makeExceptionThrowingHandler();

    EventuateKafkaConsumer consumer = makeConsumer(subscriberId, topic, handler);

    sendMessages(topic);

    assertConsumerStopped(consumer);

    assertHandlerInvokedAtLeastOnce(handler);
  }

  private EventuateKafkaConsumer makeConsumer(String subscriberId, String topic, EventuateKafkaConsumerMessageHandler handler) {
    EventuateKafkaConsumer consumer = new EventuateKafkaConsumer(subscriberId, handler, Collections.singletonList(topic), kafkaProperties.getBootstrapServers(), consumerProperties);

    consumer.start();
    return consumer;
  }

  private void sendMessages(String topic) {
    producer.send(topic, "1", "a");
    producer.send(topic, "1", "b");
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