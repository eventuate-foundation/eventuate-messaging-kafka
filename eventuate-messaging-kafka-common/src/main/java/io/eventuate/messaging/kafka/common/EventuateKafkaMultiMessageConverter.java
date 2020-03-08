package io.eventuate.messaging.kafka.common;

import io.eventuate.messaging.kafka.common.sbe.MessageHeaderDecoder;
import io.eventuate.messaging.kafka.common.sbe.MessageHeaderEncoder;
import io.eventuate.messaging.kafka.common.sbe.MultiMessageDecoder;
import io.eventuate.messaging.kafka.common.sbe.MultiMessageEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;

import java.util.*;
import java.util.stream.Collectors;

public class EventuateKafkaMultiMessageConverter {
  public static int HEADER_SIZE = MessageHeaderEncoder.ENCODED_LENGTH + MultiMessageEncoder.MessagesEncoder.HEADER_SIZE + MultiMessageEncoder.HeadersEncoder.HEADER_SIZE;
  public static final String MAGIC_ID = "a8c79db675e14c4cbf1eb77d0d6d0f00"; // generated UUID
  public static final byte[] MAGIC_ID_BYTES = EventuateBinaryMessageEncoding.stringToBytes(MAGIC_ID);

  public byte[] convertMessagesToBytes(List<EventuateKafkaMultiMessage> eventuateKafkaMultiMessages) {
    return convertMessagesToBytes(new EventuateKafkaMultiMessages(Collections.emptyList(), eventuateKafkaMultiMessages));
  }

  public byte[] convertMessagesToBytes(EventuateKafkaMultiMessages eventuateKafkaMultiMessages) {

    MessageBuilder builder = new MessageBuilder();

    builder.setHeaders(eventuateKafkaMultiMessages.getHeaders());

    for (EventuateKafkaMultiMessage message : eventuateKafkaMultiMessages.getMessages()) {
      builder.addMessage(message);
    }

    return builder.toBinaryArray();
  }

  public EventuateKafkaMultiMessages convertBytesToMessages(byte[] bytes) {

    if (!isMultiMessage(bytes)) {
      throw new RuntimeException("WRONG MAGIC NUMBER!");
    }

    MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

    MutableDirectBuffer buffer = new UnsafeBuffer(bytes);

    messageHeaderDecoder.wrap(buffer, 0);

    final int templateId = messageHeaderDecoder.templateId();
    final int actingBlockLength = messageHeaderDecoder.blockLength();
    final int actingVersion = messageHeaderDecoder.version();

    MultiMessageDecoder multiMessageDecoder = new MultiMessageDecoder().wrap(buffer, messageHeaderDecoder.encodedLength(), actingBlockLength, actingVersion);

    List<EventuateKafkaMultiMessagesHeader> headers = decodeEventuateKafkaMultiMessagesHeaders(multiMessageDecoder);
    List<EventuateKafkaMultiMessage> messages = decodeEventuateKafkaMultiMessages(multiMessageDecoder);

    return new EventuateKafkaMultiMessages(headers, messages);
  }

  private List<EventuateKafkaMultiMessagesHeader> decodeEventuateKafkaMultiMessagesHeaders(MultiMessageDecoder multiMessageDecoder) {
    MultiMessageDecoder.HeadersDecoder headersDecoder = multiMessageDecoder.headers();
    List<EventuateKafkaMultiMessagesHeader> headers = new ArrayList<>();

    for (int i = 0; i < headersDecoder.count(); i++) {
      headersDecoder.next();
      int keyLength = headersDecoder.keyLength();
      byte[] keyBytes = new byte[keyLength];
      headersDecoder.getKey(keyBytes, 0, keyLength);
      int valueLength = headersDecoder.valueLength();
      byte[] valueBytes = new byte[valueLength];
      headersDecoder.getKey(valueBytes, 0, valueLength);

      String key = EventuateBinaryMessageEncoding.bytesToString(keyBytes);
      String value = EventuateBinaryMessageEncoding.bytesToString(valueBytes);

      headers.add(new EventuateKafkaMultiMessagesHeader(key, value));
    }

    return headers;
  }

  private List<EventuateKafkaMultiMessage> decodeEventuateKafkaMultiMessages(MultiMessageDecoder multiMessageDecoder) {
    MultiMessageDecoder.MessagesDecoder messagesDecoder = multiMessageDecoder.messages();
    List<EventuateKafkaMultiMessage> messages = new ArrayList<>();

    for (int i = 0; i < messagesDecoder.count(); i++) {
      messagesDecoder.next();

      List<EventuateKafkaMultiMessageHeader> messageHeaders = decodeEventuateKafkaMultiMessageHeaders(messagesDecoder);

      int keyLength = messagesDecoder.keyLength();
      byte[] keyBytes = new byte[keyLength];
      messagesDecoder.getKey(keyBytes, 0, keyLength);
      int valueLength = messagesDecoder.valueLength();
      byte[] valueBytes = new byte[valueLength];
      messagesDecoder.getKey(valueBytes, 0, valueLength);

      String key = EventuateBinaryMessageEncoding.bytesToString(keyBytes);
      String value = EventuateBinaryMessageEncoding.bytesToString(valueBytes);

      messages.add(new EventuateKafkaMultiMessage(key, value, messageHeaders));
    }

    return messages;
  }

  private List<EventuateKafkaMultiMessageHeader> decodeEventuateKafkaMultiMessageHeaders(MultiMessageDecoder.MessagesDecoder messagesDecoder) {
    List<EventuateKafkaMultiMessageHeader> messageHeaders = new ArrayList<>();
    MultiMessageDecoder.MessagesDecoder.HeadersDecoder messageHeadersDecoder = messagesDecoder.headers();

    for (int j = 0; j < messageHeadersDecoder.count(); j++) {
      messageHeadersDecoder.next();

      int keyLength = messageHeadersDecoder.keyLength();
      byte[] keyBytes = new byte[keyLength];
      messageHeadersDecoder.getKey(keyBytes, 0, keyLength);
      int valueLength = messageHeadersDecoder.valueLength();
      byte[] valueBytes = new byte[valueLength];
      messageHeadersDecoder.getKey(valueBytes, 0, valueLength);

      String key = EventuateBinaryMessageEncoding.bytesToString(keyBytes);
      String value = EventuateBinaryMessageEncoding.bytesToString(valueBytes);

      messageHeaders.add(new EventuateKafkaMultiMessageHeader(key, value));
    }

    return messageHeaders;
  }

  public List<String> convertBytesToValues(byte[] bytes) {
    if (isMultiMessage(bytes)) {
      return convertBytesToMessages(bytes)
              .getMessages()
              .stream()
              .map(EventuateKafkaMultiMessage::getValue)
              .collect(Collectors.toList());
    }
    else {
      return Collections.singletonList(EventuateBinaryMessageEncoding.bytesToString(bytes));
    }
  }

  public boolean isMultiMessage(byte[] message) {
    if (message.length < MAGIC_ID_BYTES.length) return false;

    for (int i = 0; i < MAGIC_ID_BYTES.length; i++)
      if (message[i] != MAGIC_ID_BYTES[i]) return false;

    return true;
  }

  public static class MessageBuilder {
    private Optional<Integer> maxSize;
    private int size;
    private List<EventuateKafkaMultiMessagesHeader> headers = Collections.emptyList();
    private List<EventuateKafkaMultiMessage> messagesToWrite = new ArrayList<>();
    
    public MessageBuilder(int maxSize) {
      this(Optional.of(maxSize));
    }

    public MessageBuilder() {
      this(Optional.empty());
    }

    public MessageBuilder(Optional<Integer> maxSize) {
      this.maxSize = maxSize;

      size = HEADER_SIZE;
    }

    public int getSize() {
      return size;
    }

    public boolean setHeaders(List<EventuateKafkaMultiMessagesHeader> headers) {
      int estimatedSize = KeyValue.estimateSize(headers);

      if (isSizeOverLimit(estimatedSize)) {
        return false;
      }

      this.headers = headers;

      size += estimatedSize;

      return true;
    }

    public boolean addMessage(EventuateKafkaMultiMessage message) {
      int estimatedSize = message.estimateSize();

      if (isSizeOverLimit(estimatedSize)) {
        return false;
      }

      messagesToWrite.add(message);

      size += estimatedSize;

      return true;
    }

    private boolean isSizeOverLimit(int estimatedSize) {
      return maxSize.map(ms -> size + estimatedSize > ms).orElse(false);
    }

    public byte[] toBinaryArray() {

      ExpandableArrayBuffer buffer = new ExpandableArrayBuffer(2 * size); // Think about the size

      MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

      MessageHeaderEncoder he = messageHeaderEncoder.wrap(buffer, 0);

      for (int i = 0; i < EventuateKafkaMultiMessageConverter.MAGIC_ID_BYTES.length; i++) {
        byte b = EventuateKafkaMultiMessageConverter.MAGIC_ID_BYTES[i];
        he.magicBytes(i, b);
      }

      MultiMessageEncoder multiMessageEncoder = new MultiMessageEncoder().wrapAndApplyHeader(buffer, 0, messageHeaderEncoder);

      MultiMessageEncoder.HeadersEncoder headersEncoder = multiMessageEncoder.headersCount(headers.size());
      headers.forEach(header -> headersEncoder.next().key(header.getKey()).value(header.getValue()));

      MultiMessageEncoder.MessagesEncoder messagesEncoder = multiMessageEncoder.messagesCount(messagesToWrite.size());
      messagesToWrite.forEach(message -> {
        messagesEncoder.next();

        MultiMessageEncoder.MessagesEncoder.HeadersEncoder messageHeadersEncoder =
                messagesEncoder.headersCount(message.getHeaders().size());

        for (int i = 0; i < message.getHeaders().size(); i++) {
          EventuateKafkaMultiMessageHeader header = message.getHeaders().get(i);
          messageHeadersEncoder.next().key(header.getKey()).value(header.getValue());
        }

        messagesEncoder.key(message.getKey()).value(message.getValue());
      });

      return Arrays.copyOfRange(buffer.byteArray(), 0, multiMessageEncoder.encodedLength() + messageHeaderEncoder.encodedLength());
    }
  }
}
