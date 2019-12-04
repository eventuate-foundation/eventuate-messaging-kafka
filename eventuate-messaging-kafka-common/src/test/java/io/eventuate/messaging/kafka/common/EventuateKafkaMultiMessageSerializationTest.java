package io.eventuate.messaging.kafka.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EventuateKafkaMultiMessageSerializationTest {

  private static final List<EventuateKafkaMultiMessageKeyValue> MESSAGES = Arrays.asList(new EventuateKafkaMultiMessageKeyValue("key1", "value1"), new EventuateKafkaMultiMessageKeyValue("key2", "value2"));
  private static final List<EventuateKafkaMultiMessageKeyValue> NULL_MESSAGES = Arrays.asList(new EventuateKafkaMultiMessageKeyValue(null, null));

  @Test
  public void testMessageConverterRegularMessages() {
    testMessageBuilder(MESSAGES);
  }

  @Test
  public void testMessageConverterNullKeyAndValueHandling() {
    testMessageBuilder(NULL_MESSAGES);
  }

  @Test
  public void testMessageBuilderRegularMessages() {
    testMessageBuilder(MESSAGES);
  }

  @Test
  public void testMessageBuilderNullMessages() {
    testMessageBuilder(NULL_MESSAGES);
  }

  @Test
  public void testMessageBuilderSizeCheck() {
    int sizeOfHeaderAndFirstMessage = EventuateKafkaMultiMessageBuilder.MAGIC_ID_BYTES.length
            + 2 * 4
            + MESSAGES.get(0).getKey().getBytes().length
            + MESSAGES.get(0).getValue().getBytes().length;

    EventuateKafkaMultiMessageBuilder eventuateMultiMessageBuilder = new EventuateKafkaMultiMessageBuilder(sizeOfHeaderAndFirstMessage);

    Assert.assertTrue(eventuateMultiMessageBuilder.addMessage(MESSAGES.get(0)));
    Assert.assertFalse(eventuateMultiMessageBuilder.addMessage(MESSAGES.get(1)));
  }

  @Test
  public void testMessageBuilderHeaderSizeCheck() {
    int sizeOfFirstMessage = 2 * 4
            + MESSAGES.get(0).getKey().getBytes().length
            + MESSAGES.get(0).getValue().getBytes().length;

    EventuateKafkaMultiMessageBuilder eventuateMultiMessageBuilder = new EventuateKafkaMultiMessageBuilder(sizeOfFirstMessage);

    Assert.assertFalse(eventuateMultiMessageBuilder.addMessage(MESSAGES.get(0)));
  }

  public void testMessageBuilder(List<EventuateKafkaMultiMessageKeyValue> messages) {
    EventuateKafkaMultiMessageBuilder eventuateMultiMessageBuilder = new EventuateKafkaMultiMessageBuilder(1000000);
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    Assert.assertTrue(messages.stream().allMatch(eventuateMultiMessageBuilder::addMessage));

    byte[] serializedMessages = eventuateMultiMessageBuilder.toBinaryArray();

    List<EventuateKafkaMultiMessageKeyValue> deserializedMessages = eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);

    Assert.assertEquals(messages, deserializedMessages);
  }

  public void testMessageConverter(List<EventuateKafkaMultiMessageKeyValue> messages) {
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    byte[] serializedMessages = eventuateMultiMessageConverter.convertMessagesToBytes(messages);

    List<EventuateKafkaMultiMessageKeyValue> deserializedMessages = eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);

    Assert.assertEquals(messages, deserializedMessages);
  }
}
