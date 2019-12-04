package io.eventuate.messaging.kafka.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EventuateKafkaMultiMessageSerializationTest {

  private static final List<EventuateKafkaMultiMessageKeyValue> MESSAGES = Arrays.asList(new EventuateKafkaMultiMessageKeyValue("key1", "value1"), new EventuateKafkaMultiMessageKeyValue("key2", "value2"));

  @Test
  public void testMessageConverter() {
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    byte[] serializedMessages = eventuateMultiMessageConverter.convertMessagesToBytes(MESSAGES);

    List<EventuateKafkaMultiMessageKeyValue> deserializedMessages = eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);

    Assert.assertEquals(MESSAGES, deserializedMessages);
  }

  @Test
  public void testMessageBuilder() {
    EventuateKafkaMultiMessageBuilder eventuateMultiMessageBuilder = new EventuateKafkaMultiMessageBuilder(1000000);
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    Assert.assertTrue(MESSAGES.stream().allMatch(eventuateMultiMessageBuilder::addMessage));

    byte[] serializedMessages = eventuateMultiMessageBuilder.toBinaryArray();

    List<EventuateKafkaMultiMessageKeyValue> deserializedMessages = eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);

    Assert.assertEquals(MESSAGES, deserializedMessages);
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
}
