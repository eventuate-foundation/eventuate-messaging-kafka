package io.eventuate.messaging.kafka.common;

import io.eventuate.messaging.kafka.common.sbe.MessageHeaderDecoder;
import io.eventuate.messaging.kafka.common.sbe.MessageHeaderEncoder;
import io.eventuate.messaging.kafka.common.sbe.MultiMessageDecoder;
import io.eventuate.messaging.kafka.common.sbe.MultiMessageEncoder;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.ExpandableDirectByteBuffer;

import java.nio.ByteBuffer;
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
    ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);

    if (!isMagicIdPresent(byteBuffer)) {
      throw new RuntimeException("WRONG MAGIC NUMBER!");
    }

    MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

    ExpandableDirectByteBuffer buffer = new ExpandableDirectByteBuffer();
    buffer.putBytes(0, bytes);

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

  private boolean isMagicIdPresent(ByteBuffer byteBuffer) {
    if (byteBuffer.remaining() < MAGIC_ID_BYTES.length) return false;

    for (int i = 0; i < MAGIC_ID_BYTES.length; i++) {
      if (MAGIC_ID_BYTES[i] != byteBuffer.get()) return false;
    }

    return true;
  }

  public static class MessageBuilder {
    private Optional<Integer> maxSize;
    private int size;
    private List<EventuateKafkaMultiMessageKeyValue> messagesToWrite = new ArrayList<>();

    MultiMessageEncoder multiMessageEncoder;
    ExpandableArrayBuffer buffer;

    public MessageBuilder(int maxSize) {
      this(Optional.of(maxSize));
    }

    public MessageBuilder() {
      this(Optional.empty());
    }

    public MessageBuilder(Optional<Integer> maxSize) {
      this.maxSize = maxSize;

      size = MessageHeaderEncoder.ENCODED_LENGTH + MultiMessageEncoder.MessagesEncoder.HEADER_SIZE;

      buffer = new ExpandableArrayBuffer(1000);
      MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();

      for (int i = 0; i < EventuateKafkaMultiMessageConverter.MAGIC_ID_BYTES.length; i++) {
        byte b = EventuateKafkaMultiMessageConverter.MAGIC_ID_BYTES[i];
        messageHeaderEncoder.wrap(buffer, 0).magicBytes(i, b);
      }

      multiMessageEncoder = new MultiMessageEncoder().wrapAndApplyHeader(buffer, 0, messageHeaderEncoder);
    }

    public int getSize() {
      return size;
    }

    public boolean addMessage(EventuateKafkaMultiMessageKeyValue message) {
      int keyLength = message.getKey() == null ? 0 : message.getKey().length() * 2;
      int valueLength = message.getValue() == null ? 0 : message.getValue().length() * 2;
      int additionalSize = 2 * 4 + keyLength + valueLength;

      if (maxSize.map(ms -> size + additionalSize > ms).orElse(false)) {
        return false;
      }

      messagesToWrite.add(message);

      size += additionalSize;

      return true;
    }

    public byte[] toBinaryArray() {
      MultiMessageEncoder.MessagesEncoder messagesEncoder = multiMessageEncoder.messagesCount(messagesToWrite.size());

      messagesToWrite.forEach(message -> messagesEncoder.next().key(message.getKey()).value(message.getValue()));

      return Arrays.copyOfRange(buffer.byteArray(), 0, size);
    }
  }
}
