package io.eventuate.messaging.kafka.spring.basic.consumer;

import io.eventuate.messaging.kafka.basic.consumer.*;
import io.eventuate.messaging.kafka.common.EventuateBinaryMessageEncoding;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducer;
import io.eventuate.messaging.kafka.producer.EventuateKafkaProducerConfigurationProperties;
import io.eventuate.messaging.kafka.spring.producer.EventuateKafkaProducerSpringConfigurationPropertiesConfiguration;
import io.eventuate.util.test.async.Eventually;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.StreamUtils;

import java.io.IOException;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@SpringBootTest(classes = EventuateKafkaConsumerTest.Config.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class EventuateKafkaConsumerTest {

  @Configuration
  @Import({EventuateKafkaConsumerSpringConfigurationPropertiesConfiguration.class,
          EventuateKafkaProducerSpringConfigurationPropertiesConfiguration.class})
  public static class Config {
  }

  private Logger logger = LoggerFactory.getLogger(getClass());

  @Value("${eventuatelocal.kafka.bootstrap.servers}")
  private String bootstrapServers;

  @Autowired
  private EventuateKafkaConsumerConfigurationProperties eventuateKafkaConsumerConfigurationProperties;

  @Autowired
  private EventuateKafkaProducerConfigurationProperties eventuateKafkaProducerConfigurationProperties;

  private EventuateKafkaProducer eventuateKafkaProducer;

  private EventuateKafkaConsumerMessageHandler mockedHandler;

  private String subscriberId = uniqueId();

  private String uniqueId() {
    return UUID.randomUUID().toString();
  }

  private String topic = uniqueId();

  private LinkedBlockingQueue<byte[]> queue = new LinkedBlockingQueue<>();

  private KafkaConsumerFactory kafkaConsumerFactory = new DefaultKafkaConsumerFactory();

  @BeforeEach
  public void init() {
    eventuateKafkaProducer = new EventuateKafkaProducer(bootstrapServers, eventuateKafkaProducerConfigurationProperties);

    subscriberId = uniqueId();
    topic = uniqueId();
    mockedHandler = mock(EventuateKafkaConsumerMessageHandler.class);
  }

  @AfterEach
  public void after() throws Exception {
    startKafka();
    waitForKafka();
  }

  @Test
  public void testHandledConsumerException() {
    when(mockedHandler.apply(any(), any())).then(invocation -> {
      ((BiConsumer<Void, Throwable>) invocation.getArguments()[1]).accept(null, new RuntimeException("Something happend"));
      return null;
    });
    createConsumer(mockedHandler);
    sendMessage();
    assertHandlerInvoked();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testUnhandledConsumerException() {
    when(mockedHandler.apply(any(), any())).thenThrow(new RuntimeException("Something happened!"));
    createConsumer(mockedHandler);
    sendMessage();
    assertHandlerInvoked();
    assertMessageReceivedByNewConsumer();
  }

  /*
  /
  public void testConsumerSwitchOnHanging() {
    createConsumer(mockedHandler);
    sendMessage();
    assertHandlerInvoked();
    assertMessageReceivedByNewConsumer();
  }
  */

  @Test
  public void testConsumerStop() {
    EventuateKafkaConsumer eventuateKafkaConsumer = createConsumer(mockedHandler);
    sendMessage();
    assertHandlerInvoked();
    eventuateKafkaConsumer.stop();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testNotClosedConsumerOnStop() {
    eventuateKafkaConsumerConfigurationProperties.getProperties().put("max.poll.interval.ms", "1000");
    EventuateKafkaConsumer eventuateKafkaConsumer = createConsumer(mockedHandler);
    sendMessage();
    assertHandlerInvoked();
    eventuateKafkaConsumer.setCloseConsumerOnStop(false);
    eventuateKafkaConsumer.stop();
    assertMessageReceivedByNewConsumer();
  }

  @Test
  public void testKafkaTimeOut() throws Exception {
    eventuateKafkaConsumerConfigurationProperties.getProperties().put("default.api.timeout.ms", "3000");

    EventuateKafkaConsumer eventuateKafkaConsumer = createNoOpConsumer();

    Runnable onCommitOffsetsFailedCallback = mock(Runnable.class);

    AtomicBoolean justOnce = new AtomicBoolean(false);

    ConsumerCallbacks consumerCallbacks = new ConsumerCallbacks() {
      @Override
      public void onTryCommitCallback() {
        if (!isItCalledFromPollingLoop()) {
          return;
        }

        if (justOnce.compareAndSet(false, true))
          try {
            stopKafka();
            waitForKafkaStop();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
      }

      @Override
      public void onCommitedCallback() {
        if (!isItCalledFromPollingLoop()) {
          return;
        }

        eventuateKafkaConsumer.stop();
        sendMessage("test-value-2");
      }

      @Override
      public void onCommitFailedCallback() {
        onCommitOffsetsFailedCallback.run();
      }

      private boolean isItCalledFromPollingLoop() {
        return Arrays
                .stream(Thread.currentThread().getStackTrace())
                .anyMatch(stackTraceElement -> stackTraceElement.getMethodName().equals("runPollingLoop"));
      }
    };

    eventuateKafkaConsumer.setConsumerCallbacks(Optional.of(consumerCallbacks));

    sendMessage();

    Eventually.eventually(60, 500, TimeUnit.MILLISECONDS, () ->
      verify(onCommitOffsetsFailedCallback).run());

    startKafka();
    waitForKafka();

    assertMessageReceivedByNewConsumer("test-value-2");
  }

  private void stopKafka() throws Exception {
    executeOsCommand("docker-compose stop kafka");
  }

  private void startKafka() throws Exception {
    executeOsCommand("docker-compose start kafka");
  }

  private void executeOsCommand(String command) throws Exception {
    Process p = Runtime.getRuntime().exec(command);

    try {
      assertTrue(p.waitFor(30, TimeUnit.SECONDS));

      assertEquals(0, p.exitValue());

      String output = StreamUtils.copyToString(p.getInputStream(), Charset.defaultCharset());
      String errors = StreamUtils.copyToString(p.getErrorStream(), Charset.defaultCharset());

      logger.info("%s output: %s".formatted(command, output));
      logger.error("%s errors: %s".formatted(command, errors));

    } finally {
      p.destroy();
    }
  }

  private void waitForKafka() {
    Eventually.eventually(60, 500, TimeUnit.MILLISECONDS, () -> {
      String host = getKafkaHost();
      try (Socket ignored = new Socket(host, getKafkaPort())) {
        // do nothing
      } catch (IOException e) {
        throw new RuntimeException("Can't connect to host: " + host, e);
      }
    });

    System.out.println("Connected to kafka port");

    Eventually.eventually(60, 500, TimeUnit.MILLISECONDS, () -> {
      System.out.println("Trying to subscribe");
      String x = uniqueId();
      EventuateKafkaConsumer eventuateKafkaConsumer = new EventuateKafkaConsumer(x,
              (record, callback) -> {
                callback.accept(null, null);
                return null;
              },
              Collections.singletonList(x),
              bootstrapServers,
              eventuateKafkaConsumerConfigurationProperties,
              kafkaConsumerFactory);

      eventuateKafkaConsumer.start();

      EventuateKafkaConsumer consumer = eventuateKafkaConsumer;
      consumer.stop();
    });
    System.out.println("Subscribed");

  }

  private void waitForKafkaStop() {
    Eventually.eventually(60, 500, TimeUnit.MILLISECONDS, () -> {
      try (Socket ignored = new Socket(getKafkaHost(), getKafkaPort())) {
        throw new IllegalStateException("Kafka was not stopped!");
      } catch (IOException ignored) {
      }
    });
  }

  private String getKafkaHost() {
    return bootstrapServers.split(":")[0];
  }

  private int getKafkaPort() {
    return Integer.parseInt(bootstrapServers.split(":")[1]);
  }

  private void assertMessageReceivedByNewConsumer() {
    assertMessageReceivedByNewConsumer("test-value");
  }

  private void assertMessageReceivedByNewConsumer(String value) {
    createConsumer((record, callback) -> {
      queue.add(record.value());
      callback.accept(null, null);
      return null;
    });

    byte[] m;
    try {
      m = queue.poll(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertNotNull(m, "Message not received by timeout");
    String message = EventuateBinaryMessageEncoding.bytesToString(m);
    Assertions.assertEquals(value, message);
  }

  private void assertHandlerInvoked() {
    Eventually.eventually(() -> verify(mockedHandler).apply(any(), any()));
  }

  private void sendMessage() {
    sendMessage("test-value");
  }

  private void sendMessage(String value) {
    try {
      eventuateKafkaProducer.send(topic, "test-key", value).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }

  private EventuateKafkaConsumer createConsumer(EventuateKafkaConsumerMessageHandler handler) {
    EventuateKafkaConsumer eventuateKafkaConsumer = new EventuateKafkaConsumer(subscriberId,
            handler,
            Collections.singletonList(topic),
            bootstrapServers,
            eventuateKafkaConsumerConfigurationProperties,
            kafkaConsumerFactory
    );

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
