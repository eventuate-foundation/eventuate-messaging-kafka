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
    int sizeOfHeaderAndFirstMessage = EventuateKafkaMultiMessageConverter.MAGIC_ID_BYTES.length
            + 2 * 4
            + EventuateBinaryMessageEncoding.stringToBytes(MESSAGES.get(0).getKey()).length
            + EventuateBinaryMessageEncoding.stringToBytes(MESSAGES.get(0).getValue()).length;

    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder =
            new EventuateKafkaMultiMessageConverter.MessageBuilder(sizeOfHeaderAndFirstMessage);

    Assert.assertTrue(messageBuilder.addMessage(MESSAGES.get(0)));
    Assert.assertFalse(messageBuilder.addMessage(MESSAGES.get(1)));
  }

  @Test
  public void testMessageBuilderHeaderSizeCheck() {
    int sizeOfFirstMessage = 2 * 4
            + EventuateBinaryMessageEncoding.stringToBytes(MESSAGES.get(0).getKey()).length
            + EventuateBinaryMessageEncoding.stringToBytes(MESSAGES.get(0).getValue()).length;

    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder =
            new EventuateKafkaMultiMessageConverter.MessageBuilder(sizeOfFirstMessage);

    Assert.assertFalse(messageBuilder.addMessage(MESSAGES.get(0)));
  }

  public void testMessageBuilder(List<EventuateKafkaMultiMessageKeyValue> messages) {
    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder = new EventuateKafkaMultiMessageConverter.MessageBuilder(1000000);
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    Assert.assertTrue(messages.stream().allMatch(messageBuilder::addMessage));

    byte[] serializedMessages = messageBuilder.toBinaryArray();

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
