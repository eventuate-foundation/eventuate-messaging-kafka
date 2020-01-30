package io.eventuate.messaging.kafka.common;

import io.eventuate.messaging.kafka.common.sbe.MessageHeaderEncoder;
import io.eventuate.messaging.kafka.common.sbe.MultiMessageEncoder;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class EventuateKafkaMultiMessageConverterTest {

  private static final List<EventuateKafkaMultiMessagesHeader> HEADERS =
          new ArrayList<>(Arrays.asList(new EventuateKafkaMultiMessagesHeader("commonheader1key", "commonheader1value"), new EventuateKafkaMultiMessagesHeader("commonheader2key", "commonheader2value")));

  private static final List<EventuateKafkaMultiMessage> TWO_BYTE_CHARACTER_MESSAGES = Collections.singletonList(new EventuateKafkaMultiMessage("ключ", "значение"));
  private static final List<EventuateKafkaMultiMessage> THREE_BYTE_CHARACTER_MESSAGES = Collections.singletonList(new EventuateKafkaMultiMessage("罗杰·费德勒真棒!", "德约科维奇摇滚！"));
  private static final EventuateKafkaMultiMessage THREE_BYTE_CHARACTER_MESSAGES_0 = THREE_BYTE_CHARACTER_MESSAGES.get(0);

  private static final String MESSAGE_0_KEY = "key1";
  private static final String MESSAGE_0_VALUE = "value1";
  private static final String MESSAGE_1_KEY = "key2";
  private static final String MESSAGE_1_VALUE = "value2";

  private static final List<EventuateKafkaMultiMessage> SIMPLE_MESSAGES =
          Arrays.asList(new EventuateKafkaMultiMessage(MESSAGE_0_KEY, MESSAGE_0_VALUE), new EventuateKafkaMultiMessage(MESSAGE_1_KEY, MESSAGE_1_VALUE));

  private static final List<EventuateKafkaMultiMessage> MESSAGES_WITH_HEADERS =
          Arrays.asList(new EventuateKafkaMultiMessage(MESSAGE_0_KEY, MESSAGE_0_VALUE, new ArrayList<>(Arrays.asList(new EventuateKafkaMultiMessageHeader("header1key", "header1value"), new EventuateKafkaMultiMessageHeader("header2key", "header2value")))),
                  new EventuateKafkaMultiMessage(MESSAGE_1_KEY, MESSAGE_1_VALUE, new ArrayList<>(Arrays.asList(new EventuateKafkaMultiMessageHeader("header3key", "header3value"), new EventuateKafkaMultiMessageHeader("header4key", "header4value")))));

  private static final List<EventuateKafkaMultiMessage> EMPTY_MESSAGES = Collections.singletonList(new EventuateKafkaMultiMessage("", ""));

  private static final List<EventuateKafkaMultiMessage> NULL_MESSAGES = Collections.singletonList(new EventuateKafkaMultiMessage(null, null));

  private byte[] serializedMessages;
  private int estimatedSize;

  @Test
  public void testMessageBuilderSimpleMessages() {
    testMessageBuilder(new EventuateKafkaMultiMessages(SIMPLE_MESSAGES));
  }

  @Test
  public void testMessageBuilder2ByteCharacterMessages() {
    testMessageBuilder(new EventuateKafkaMultiMessages(TWO_BYTE_CHARACTER_MESSAGES));
    assertEstimatedGreaterOrEqualToActual();
  }

  private void assertEstimatedGreaterOrEqualToActual() {
    Assert.assertThat(estimatedSize, Matchers.greaterThanOrEqualTo(serializedMessages.length));
  }

  @Test
  public void testMessageBuilder3ByteCharacterMessages() throws UnsupportedEncodingException {

    Assert.assertThat(THREE_BYTE_CHARACTER_MESSAGES_0.getKey().getBytes(StandardCharsets.UTF_8).length, Matchers.greaterThan(THREE_BYTE_CHARACTER_MESSAGES_0.getKey().length() * 2));
    Assert.assertThat(THREE_BYTE_CHARACTER_MESSAGES_0.getValue().getBytes(StandardCharsets.UTF_8).length, Matchers.greaterThan(THREE_BYTE_CHARACTER_MESSAGES_0.getValue().length() * 2));
    testMessageBuilder(new EventuateKafkaMultiMessages(THREE_BYTE_CHARACTER_MESSAGES));
    assertEstimatedGreaterOrEqualToActual();
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
    int sizeOfFirstMessage =
            KeyValue.KEY_HEADER_SIZE + MESSAGE_0_KEY.length() * KeyValue.ESTIMATED_BYTES_PER_CHAR +
            KeyValue.VALUE_HEADER_SIZE + MESSAGE_0_VALUE.length() * KeyValue.ESTIMATED_BYTES_PER_CHAR;

    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder =
            new EventuateKafkaMultiMessageConverter.MessageBuilder(sizeOfFirstMessage);

    Assert.assertFalse(messageBuilder.addMessage(SIMPLE_MESSAGES.get(0)));
  }

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

  public void testMessageBuilder(EventuateKafkaMultiMessages messages) {
    testMessageBuilder(messages, messages);
  }

  public void testMessageBuilder(EventuateKafkaMultiMessages original, EventuateKafkaMultiMessages result) {
    EventuateKafkaMultiMessageConverter.MessageBuilder messageBuilder = new EventuateKafkaMultiMessageConverter.MessageBuilder(1000000);
    EventuateKafkaMultiMessageConverter eventuateMultiMessageConverter = new EventuateKafkaMultiMessageConverter();

    Assert.assertTrue(messageBuilder.setHeaders(original.getHeaders()));
    Assert.assertTrue(original.getMessages().stream().allMatch(messageBuilder::addMessage));

    serializedMessages = messageBuilder.toBinaryArray();
    estimatedSize = EventuateKafkaMultiMessageConverter.HEADER_SIZE + original.estimateSize();

    assertEstimatedGreaterOrEqualToActual();

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
