package io.eventuate.messaging.kafka.common;

import io.eventuate.messaging.kafka.common.sbe.MessageHeaderEncoder;
import io.eventuate.messaging.kafka.common.sbe.MultiMessageEncoder;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EventuateKafkaMultiMessageConverterTest {

  private static final List<EventuateKafkaMultiMessageKeyValue> MESSAGES = Arrays.asList(new EventuateKafkaMultiMessageKeyValue("key1", "value1"), new EventuateKafkaMultiMessageKeyValue("key2", "value2"));
  private static final List<EventuateKafkaMultiMessageKeyValue> EMPTY_MESSAGES = Arrays.asList(new EventuateKafkaMultiMessageKeyValue("", ""));
  private static final List<EventuateKafkaMultiMessageKeyValue> NULL_MESSAGES = Arrays.asList(new EventuateKafkaMultiMessageKeyValue(null, null));

  private static int nMessagesForPerformanceTest = 1000;
  private static int nIterationsForPerformanceTest = 1000;
  private List<EventuateKafkaMultiMessageKeyValue> messagesForPerformanceTest;

  @Test
  public void testSBE() {
    testPerformance(() -> {
      EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder = new EventuateKafkaMultiMessageConverter.MessageBuilder(1000000);
      EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

      messagesForPerformanceTest.forEach(messageBuilder::addMessage);

      byte[] serializedMessages = messageBuilder.toBinaryArray();

      eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);
    });
  }

  @Test
  public void testRegularPerformance() {
    testPerformance(() -> {
      EventuateKafkaOldMultiMessageConverter.MessageBuilder messageBuilder = new EventuateKafkaOldMultiMessageConverter.MessageBuilder(1000000);
      EventuateKafkaOldMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaOldMultiMessageConverter();

      messagesForPerformanceTest.forEach(messageBuilder::addMessage);

      byte[] serializedMessages = messageBuilder.toBinaryArray();

      eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);
    });
  }

  private void testPerformance(Runnable runnable) {
    messagesForPerformanceTest = new ArrayList<>();

    List<Double> times = new ArrayList<>();
    int counter = 0;
    for (int i = 0; i < nIterationsForPerformanceTest; i++) {
      messagesForPerformanceTest = new ArrayList<>();
      for (int j = 0; j < nMessagesForPerformanceTest; j++) {
        messagesForPerformanceTest.add(new EventuateKafkaMultiMessageKeyValue("key" + counter, "value" + counter++));
      }
      double t = System.nanoTime();
      runnable.run();
      times.add((System.nanoTime() - t) / 1000000);
    }

    double min = times.stream().min(Double::compareTo).get();
    double max = times.stream().max(Double::compareTo).get();
    double average = times.stream().reduce((a, b) -> a + b).get() / times.size();
    times.remove(min);
    times.remove(max);
    double averageWithoutMinAndMax = times.stream().reduce((a, b) -> a + b).get() / times.size();
    System.out.println(String.format("min = %s", min));
    System.out.println(String.format("max = %s", max));
    System.out.println(String.format("average = %s", average));
    System.out.println(String.format("average without min and max = %s", averageWithoutMinAndMax));
  }

  @Test
  public void testMessageConverterRegularMessages() {
    testMessageConverter(MESSAGES);
  }

  @Test
  public void testMessageConverterNullMessages() {
    testMessageConverter(NULL_MESSAGES, EMPTY_MESSAGES);
  }

  @Test
  public void testMessageConverterEmptyMessages() {
    testMessageConverter(EMPTY_MESSAGES, EMPTY_MESSAGES);
  }

  @Test
  public void testMessageBuilderRegularMessages() {
    testMessageBuilder(MESSAGES);
  }

  @Test
  public void testMessageBuilderNullMessages() {
    testMessageBuilder(NULL_MESSAGES, EMPTY_MESSAGES);
  }

  @Test
  public void testMessageBuilderEmptyMessages() {
    testMessageBuilder(EMPTY_MESSAGES, EMPTY_MESSAGES);
  }

  @Test
  public void testMessageBuilderSizeCheck() {
    int sizeOfHeaderAndFirstMessage = MessageHeaderEncoder.ENCODED_LENGTH + MultiMessageEncoder.MessagesEncoder.HEADER_SIZE
            + 2 * 4
            + MESSAGES.get(0).getKey().length() * 2
            + MESSAGES.get(0).getValue().length() * 2;

    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder =
            new EventuateKafkaMultiMessageConverter.MessageBuilder(sizeOfHeaderAndFirstMessage);

    Assert.assertTrue(messageBuilder.addMessage(MESSAGES.get(0)));
    Assert.assertFalse(messageBuilder.addMessage(MESSAGES.get(1)));
  }

  @Test
  public void testMessageBuilderHeaderSizeCheck() {
    int sizeOfFirstMessage = 2 * 4
            + MESSAGES.get(0).getKey().length() * 2
            + MESSAGES.get(0).getValue().length() * 2;

    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder =
            new EventuateKafkaMultiMessageConverter.MessageBuilder(sizeOfFirstMessage);

    Assert.assertFalse(messageBuilder.addMessage(MESSAGES.get(0)));
  }

  public void testMessageBuilder(List<EventuateKafkaMultiMessageKeyValue> messages) {
    testMessageBuilder(messages, messages);
  }

  public void testMessageBuilder(List<EventuateKafkaMultiMessageKeyValue> original, List<EventuateKafkaMultiMessageKeyValue> result) {
    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder = new EventuateKafkaMultiMessageConverter.MessageBuilder(1000000);
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    Assert.assertTrue(original.stream().allMatch(messageBuilder::addMessage));

    byte[] serializedMessages = messageBuilder.toBinaryArray();

    List<EventuateKafkaMultiMessageKeyValue> deserializedMessages = eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);

    Assert.assertEquals(result, deserializedMessages);
  }

  public void testMessageConverter(List<EventuateKafkaMultiMessageKeyValue> messages) {
    testMessageConverter(messages, messages);
  }

  public void testMessageConverter(List<EventuateKafkaMultiMessageKeyValue> original, List<EventuateKafkaMultiMessageKeyValue> result) {
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    byte[] serializedMessages = eventuateMultiMessageConverter.convertMessagesToBytes(original);

    List<EventuateKafkaMultiMessageKeyValue> deserializedMessages = eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);

    Assert.assertEquals(result, deserializedMessages);
  }
}
