package io.eventuate.messaging.kafka.basic.consumer;

import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.util.test.async.Eventually;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = EventuateKafkaConsumerTest.Config.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class EventuateKafkaConsumerTest {

  @EnableConfigurationProperties({EventuateKafkaConsumerConfigurationProperties.class,
          EventuateKafkaProducerConfigurationProperties.class})
  public static class Config {
  }

  @Value("${eventuatelocal.kafka.bootstrap.servers}")
  private String bootstrapServers;

  @Value("${start.kafka.command}")
  private String startKafkaCommand;

  @Value("${stop.kafka.command}")
  private String stopKafkaCommand;

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;

  @Autowired
  private EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties;

  private EventuateKafkaProducer eventuateKafkaProducer;

  private EventuateKafkaConsumerMessageHandler mockedHandler;

  private String subscriberId = UUID.randomUUID().toString();
  private String topic = UUID.randomUUID().toString();

  private LinkedBlockingQueue<String> queue = new LinkedBlockingQueue<>();

  @Before
  public void init() {
    eventuateKafkaProducer = new EventuateKafkaProducer(bootstrapServers, eventuateKafkaProducerConfigurationProperties);

    subscriberId = UUID.randomUUID().toString();
    topic = UUID.randomUUID().toString();
    mockedHandler = mock(EventuateKafkaConsumerMessageHandler.class);
  }

  @After
  public void after() throws IOException {
    startKafka();
  }

  @Test
  public void testHandledConsumerException() throws InterruptedException {
    when(mockedHandler.apply(any(), any())).then(invocation -> {
      ((BiConsumer<Void, Throwable>)invocation.getArguments()[1]).accept(null, new RuntimeException("Something happend"));
      return null;
    });
    createConsumer(mockedHandler);
    sendMessage();
    assertRecordHandled();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testUnhandledConsumerException() throws InterruptedException {
    when(mockedHandler.apply(any(), any())).thenThrow(new RuntimeException("Something happened!"));
    createConsumer(mockedHandler);
    sendMessage();
    assertRecordHandled();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testConsumerSwitchOnHanging() throws InterruptedException {
    createConsumer(mockedHandler);
    sendMessage();
    assertRecordHandled();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testConsumerStop() throws InterruptedException {
    EventuateKafkaConsumer eventuateKafkaConsumer = createConsumer(mockedHandler);
    sendMessage();
    assertRecordHandled();
    eventuateKafkaConsumer.stop();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testNotClosedConsumerOnStop() throws InterruptedException {
    eventuateKafkaConsumerConfigurationProperties.getProperties().put("max.poll.interval.ms", "1000");
    EventuateKafkaConsumer eventuateKafkaConsumer = createConsumer(mockedHandler);
    sendMessage();
    assertRecordHandled();
    eventuateKafkaConsumer.closeConsumerOnStop = false;
    eventuateKafkaConsumer.stop();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testKafkaTimeOut() throws Exception {
    eventuateKafkaConsumerConfigurationProperties.getProperties().put("default.api.timeout.ms", "3000");

    EventuateKafkaConsumer eventuateKafkaConsumer = createNoOpConsumer();

    stopKafkaWhenConsumerTriesToCommitOffset(eventuateKafkaConsumer);

    Runnable onCommitOffsetsFailedCallback = Mockito.mock(Runnable.class);
    eventuateKafkaConsumer.onCommitFailedCallback = Optional.of(onCommitOffsetsFailedCallback);
    sendMessage();


    Eventually.eventually(() -> {
      Mockito.verify(onCommitOffsetsFailedCallback).run();
    });

    startKafka();

    eventuateKafkaConsumer.onCommitedCallback = Optional.of(() -> {
        eventuateKafkaConsumer.stop();
        sendMessage("test-value-2");
    });

    assertMessageReceivedByNewConsumer("test-value-2");
  }

  private void stopKafkaWhenConsumerTriesToCommitOffset(EventuateKafkaConsumer eventuateKafkaConsumer) {
    eventuateKafkaConsumer.onTryCommitCallback = Optional.of(() -> {
      try {
        stopKafka();
        Thread.sleep(3000);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private void stopKafka() throws IOException {
    Runtime.getRuntime().exec(stopKafkaCommand);
  }

  private void startKafka() throws IOException {
    Runtime.getRuntime().exec(startKafkaCommand);
  }

  private void assertMessageReceivedByNewConsumer() throws InterruptedException {
    assertMessageReceivedByNewConsumer("test-value");
  }

  private void assertMessageReceivedByNewConsumer(String value) throws InterruptedException {
    createConsumer((record, callback) -> {
      queue.add(record.value());
      callback.accept(null, null);
      return null;
    });

    String message = queue.poll(30, TimeUnit.SECONDS);
    Assert.assertEquals(value, message);
  }

  private void assertRecordHandled() {
    Eventually.eventually(() -> verify(mockedHandler).apply(any(), any()));
  }

  private void sendMessage() {
    sendMessage("test-value");
  }

  private void sendMessage(String value) {
    eventuateKafkaProducer.send(topic, "test-key", value);
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

  private EventuateKafkaConsumer createNoOpConsumer() {
    return createConsumer((record, callback) -> {
      callback.accept(null, null);
      return null;
    });
  }
}
