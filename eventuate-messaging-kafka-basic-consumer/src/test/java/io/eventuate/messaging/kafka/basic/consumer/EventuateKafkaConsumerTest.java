package io.eventuate.messaging.kafka.basic.consumer;

import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.util.test.async.Eventually;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EventuateKafkaConsumerTest.Config.class)
public class EventuateKafkaConsumerTest {

  @EnableConfigurationProperties({EventuateKafkaConsumerConfigurationProperties.class,
          EventuateKafkaProducerConfigurationProperties.class})
  public static class Config {
  }

  @Value("${eventuatelocal.kafka.bootstrap.servers}")
  private String bootstrapServers;

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;

  @Autowired
  private EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties;

  private EventuateKafkaProducer eventuateKafkaProducer;

  private volatile boolean recordHandled = false;

  private String subscriberId = UUID.randomUUID().toString();
  private String topic = UUID.randomUUID().toString();

  private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();

  @Before
  public void init() {
    eventuateKafkaProducer = new EventuateKafkaProducer(bootstrapServers, eventuateKafkaProducerConfigurationProperties);

    subscriberId = UUID.randomUUID().toString();
    topic = UUID.randomUUID().toString();
    recordHandled = false;
  }

  @Test
  public void testHandledConsumerException() throws InterruptedException {
    createConsumer((record, callback) -> {
      callback.accept(null, new RuntimeException("Something happend"));
      recordHandled = true;
    });

    sendMessage();
    assertRecordHandled();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testUnhandledConsumerException() throws InterruptedException {
    createConsumer((record, callback) -> {
      try {
        throw new RuntimeException("Something happened!");
      } finally {
        recordHandled = true;
      }
    });

    sendMessage();
    assertRecordHandled();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testConsumerSwitchOnHanging() throws InterruptedException {
    createConsumer((record, callback) -> recordHandled = true);
    sendMessage();
    assertRecordHandled();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testConsumerStop() throws InterruptedException {
    EventuateKafkaConsumer eventuateKafkaConsumer = createConsumer((record, callback) -> recordHandled = true);
    sendMessage();
    assertRecordHandled();
    eventuateKafkaConsumer.stop();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testNotClosedConsumerOnStop() throws InterruptedException {
    eventuateKafkaConsumerConfigurationProperties.getProperties().put("max.poll.interval.ms", "1000");
    EventuateKafkaConsumer eventuateKafkaConsumer = createConsumer((record, callback) -> recordHandled = true);
    sendMessage();
    assertRecordHandled();
    eventuateKafkaConsumer.closeConsumerOnStop = false;
    eventuateKafkaConsumer.stop();
    assertMessageReceivedByNewConsumer();
  }

  private void assertMessageReceivedByNewConsumer() throws InterruptedException {
    createConsumer((record, callback) -> {
      queue.add(record.value());
      callback.accept(null, null);
    });

    String message = queue.poll(30, TimeUnit.SECONDS);
    Assert.assertEquals("test-value", message);
  }

  private void assertRecordHandled() {
    Eventually.eventually(() -> Assert.assertTrue(recordHandled));
  }

  private void sendMessage() {
    eventuateKafkaProducer.send(topic, "test-key", "test-value");
  }

  private EventuateKafkaConsumer createConsumer(EventuateKafkaConsumerMessageHandler handler) {
    EventuateKafkaConsumer eventuateKafkaConsumer = new EventuateKafkaConsumer(subscriberId,
            handler,
            Collections.singletonList(topic),
            bootstrapServers,
            eventuateKafkaConsumerConfigurationProperties);

    eventuateKafkaConsumer.start();

    return eventuateKafkaConsumer;
  }
}
