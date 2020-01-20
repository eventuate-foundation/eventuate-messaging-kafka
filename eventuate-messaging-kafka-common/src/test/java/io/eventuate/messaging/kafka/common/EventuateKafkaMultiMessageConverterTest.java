package io.eventuate.messaging.kafka.common;

import io.eventuate.messaging.kafka.common.sbe.MessageHeaderEncoder;
import io.eventuate.messaging.kafka.common.sbe.MultiMessageEncoder;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class EventuateKafkaMultiMessageConverterTest {

  private static final List<EventuateKafkaMultiMessagesHeader> HEADERS =
          new ArrayList<>(Arrays.asList(new EventuateKafkaMultiMessagesHeader("commonheader1key", "commonheader1value"), new EventuateKafkaMultiMessagesHeader("commonheader2key", "commonheader2value")));

  private static final List<EventuateKafkaMultiMessage> SIMPLE_MESSAGES = Arrays.asList(new EventuateKafkaMultiMessage("key1", "value1"), new EventuateKafkaMultiMessage("key2", "value2"));

  private static final List<EventuateKafkaMultiMessage> MESSAGES_WITH_HEADERS =
          Arrays.asList(new EventuateKafkaMultiMessage("key1", "value1", new ArrayList<>(Arrays.asList(new EventuateKafkaMultiMessageHeader("header1key", "header1value"), new EventuateKafkaMultiMessageHeader("header2key", "header2value")))),
                  new EventuateKafkaMultiMessage("key2", "value2", new ArrayList<>(Arrays.asList(new EventuateKafkaMultiMessageHeader("header3key", "header3value"), new EventuateKafkaMultiMessageHeader("header4key", "header4value")))));

  private static final List<EventuateKafkaMultiMessage> EMPTY_MESSAGES = Arrays.asList(new EventuateKafkaMultiMessage("", ""));

  private static final List<EventuateKafkaMultiMessage> NULL_MESSAGES = Arrays.asList(new EventuateKafkaMultiMessage(null, null));

  @Test
  public void testMessageConverterSimpleMessages() {
    testMessageConverter(new EventuateKafkaMultiMessages(SIMPLE_MESSAGES));
  }

  @Test
  public void testMessageConverterNullMessages() {
    testMessageConverter(new EventuateKafkaMultiMessages(NULL_MESSAGES), new EventuateKafkaMultiMessages(EMPTY_MESSAGES));
  }

  @Test
  public void testMessageConverterEmptyMessages() {
    testMessageConverter(new EventuateKafkaMultiMessages(EMPTY_MESSAGES), new EventuateKafkaMultiMessages(EMPTY_MESSAGES));
  }

  @Test
  public void testMessageBuilderSimpleMessages() {
    testMessageBuilder(new EventuateKafkaMultiMessages(SIMPLE_MESSAGES));
  }

  @Test
  public void testMessageBuilderMessagesWithHeaders() {
    testMessageBuilder(new EventuateKafkaMultiMessages(HEADERS, MESSAGES_WITH_HEADERS));
  }

  @Test
  public void testMessageBuilderNullMessages() {
    testMessageBuilder(new EventuateKafkaMultiMessages(NULL_MESSAGES), new EventuateKafkaMultiMessages(EMPTY_MESSAGES));
  }

  @Test
  public void testMessageBuilderEmptyMessages() {
    testMessageBuilder(new EventuateKafkaMultiMessages(EMPTY_MESSAGES), new EventuateKafkaMultiMessages(EMPTY_MESSAGES));
  }

  @Test
  public void testMessageBuilderSizeCheck() {
    int sizeOfHeaderAndFirstMessage = MessageHeaderEncoder.ENCODED_LENGTH + MultiMessageEncoder.MessagesEncoder.HEADER_SIZE + MultiMessageEncoder.HeadersEncoder.HEADER_SIZE
            + SIMPLE_MESSAGES.get(0).estimateSize();

    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder =
            new EventuateKafkaMultiMessageConverter.MessageBuilder(sizeOfHeaderAndFirstMessage);

    Assert.assertTrue(messageBuilder.addMessage(SIMPLE_MESSAGES.get(0)));
    Assert.assertFalse(messageBuilder.addMessage(SIMPLE_MESSAGES.get(1)));
  }

  @Test
  public void testMessageBuilderHeaderSizeCheck() {
    int sizeOfFirstMessage = 2 * 4
            + SIMPLE_MESSAGES.get(0).getKey().length() * 2
            + SIMPLE_MESSAGES.get(0).getValue().length() * 2;

    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder =
            new EventuateKafkaMultiMessageConverter.MessageBuilder(sizeOfFirstMessage);

    Assert.assertFalse(messageBuilder.addMessage(SIMPLE_MESSAGES.get(0)));
  }

  public void testMessageBuilder(EventuateKafkaMultiMessages messages) {
    testMessageBuilder(messages, messages);
  }

  public void testMessageBuilder(EventuateKafkaMultiMessages original, EventuateKafkaMultiMessages result) {
    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder = new EventuateKafkaMultiMessageConverter.MessageBuilder(1000000);
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    Assert.assertTrue(messageBuilder.setHeaders(original.getHeaders()));
    Assert.assertTrue(original.getMessages().stream().allMatch(messageBuilder::addMessage));

    byte[] serializedMessages = messageBuilder.toBinaryArray();

    EventuateKafkaMultiMessages deserializedMessages = eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);

    Assert.assertEquals(result, deserializedMessages);
  }

  public void testMessageConverter(EventuateKafkaMultiMessages messages) {
    testMessageConverter(messages, messages);
  }

  public void testMessageConverter(EventuateKafkaMultiMessages original, EventuateKafkaMultiMessages result) {
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    byte[] serializedMessages = eventuateMultiMessageConverter.convertMessagesToBytes(original);

    EventuateKafkaMultiMessages deserializedMessages = eventuateMultiMessageConverter.convertBytesToMessages(serializedMessages);

    Assert.assertEquals(result, deserializedMessages);
  }
}
