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
  public static final String MAGIC_ID = "a8c79db675e14c4cbf1eb77d0d6d0f00"; // generated UUID
  public static final byte[] MAGIC_ID_BYTES = EventuateBinaryMessageEncoding.stringToBytes(MAGIC_ID);

  public byte[] convertMessagesToBytes(List<EventuateKafkaMultiMessageKeyValue> messages) {

    MessageBuilder builder = new MessageBuilder();

    for (EventuateKafkaMultiMessageKeyValue message : messages) {
      builder.addMessage(message);
    }

    return builder.toBinaryArray();
  }

  public List<EventuateKafkaMultiMessageKeyValue> convertBytesToMessages(byte[] bytes) {

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

    MultiMessageDecoder.MessagesDecoder messages = multiMessageDecoder.messages();
    int count = messages.count();

    List<EventuateKafkaMultiMessageKeyValue> result = new ArrayList<>();

    for (int i = 0; i < count; i++) {
      messages.next();
      int keyLength = messages.keyLength();
      byte[] keyBytes = new byte[keyLength];
      messages.getKey(keyBytes, 0, keyLength);
      int valueLength = messages.valueLength();
      byte[] valueBytes = new byte[valueLength];
      messages.getKey(valueBytes, 0, valueLength);

      String key = EventuateBinaryMessageEncoding.bytesToString(keyBytes);
      String value = EventuateBinaryMessageEncoding.bytesToString(valueBytes);

      result.add(new EventuateKafkaMultiMessageKeyValue(key, value));
    }

    return result;
  }

  public List<String> convertBytesToValues(byte[] bytes) {
    if (isMultiMessage(bytes)) {
      return convertBytesToMessages(bytes)
              .stream()
              .map(EventuateKafkaMultiMessageKeyValue::getValue)
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
    private List<EventuateKafkaMultiMessageKeyValue> messagesToWrite = new ArrayList<>();
    
    public MessageBuilder(int maxSize) {
      this(Optional.of(maxSize));
    }

    public MessageBuilder() {
      this(Optional.empty());
    }

    public MessageBuilder(Optional<Integer> maxSize) {
      this.maxSize = maxSize;

      size = MessageHeaderEncoder.ENCODED_LENGTH + MultiMessageEncoder.MessagesEncoder.HEADER_SIZE;

    }

    public int getSize() {
      return size;
    }

    public boolean addMessage(EventuateKafkaMultiMessageKeyValue message) {
      int estimatedSize = estimatedMessageSize(message);

      if (maxSize.map(ms -> size + estimatedSize > ms).orElse(false)) {
        return false;
      }

      messagesToWrite.add(message);

      size += estimatedSize;

      return true;
    }

    private int estimatedMessageSize(EventuateKafkaMultiMessageKeyValue message) {
      int keyLength = estimatedStringSizeInBytes(message.getKey());
      int valueLength = estimatedStringSizeInBytes(message.getValue());
      return 2 * 4 + keyLength + valueLength;
    }

    private int estimatedStringSizeInBytes(String s) {
      return s == null ? 0 : s.length() * 2;
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

      MultiMessageEncoder.MessagesEncoder messagesEncoder = multiMessageEncoder.messagesCount(messagesToWrite.size());

      messagesToWrite.forEach(message -> messagesEncoder.next().key(message.getKey()).value(message.getValue()));

      return Arrays.copyOfRange(buffer.byteArray(), 0, size);
    }
  }
}
