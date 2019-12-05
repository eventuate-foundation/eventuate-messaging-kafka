package io.eventuate.messaging.kafka.common;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class EventuateKafkaMultiMessageConverter {
  public static final String MAGIC_ID = "a8c79db675e14c4cbf1eb77d0d6d0f00"; // generated UUID
  public static final byte[] MAGIC_ID_BYTES = MAGIC_ID.getBytes(Charset.forName("UTF-8"));

  public byte[] convertMessagesToBytes(List<EventuateKafkaMultiMessageKeyValue> messages) {

    MessageBuilder builder = new MessageBuilder();

    for (EventuateKafkaMultiMessageKeyValue message : messages) {
      builder.addMessage(message);
    }

    return builder.toBinaryArray();
  }

  public List<EventuateKafkaMultiMessageKeyValue> convertBytesToMessages(byte[] bytes) {
    ByteArrayInputStream binaryStream = new ByteArrayInputStream(bytes);
    try {
      byte[] magicBytes = new byte[MAGIC_ID_BYTES.length];
      binaryStream.read(magicBytes);

      if (!Arrays.equals(magicBytes, MAGIC_ID_BYTES)) {
        throw new RuntimeException("WRONG MAGIC NUMBER!");
      }

      List<EventuateKafkaMultiMessageKeyValue> messages = new ArrayList<>();

      while (binaryStream.available() > 0) {
        String key = null;
        String value = null;

        int keyLength = readInt(binaryStream);

        if (keyLength > 0) {
          byte[] keyBytes = new byte[keyLength];
          binaryStream.read(keyBytes);
          key = new String(keyBytes, Charset.forName("UTF-8"));
        }

        int valueLength = readInt(binaryStream);
        if (valueLength > 0) {
          byte[] valueBytes = new byte[valueLength];
          binaryStream.read(valueBytes);
          value = new String(valueBytes, Charset.forName("UTF-8"));
        }

        messages.add(new EventuateKafkaMultiMessageKeyValue(key, value));
      }

      return messages;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean isMultiMessage1(byte[] message) {
    if (message.length < MAGIC_ID_BYTES.length) return false;

    return ByteBuffer.wrap(message, 0, MAGIC_ID_BYTES.length).equals(ByteBuffer.wrap(MAGIC_ID_BYTES));
  }

  private static byte[] intToBytes(int value) {
    return ByteBuffer.allocate(4).putInt(value).array();
  }

  private static int bytesToInt(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getInt();
  }

  private static int readInt(ByteArrayInputStream binaryStream) throws IOException {
    byte[] buf = new byte[4];
    binaryStream.read(buf);
    return bytesToInt(buf);
  }

  public static class MessageBuilder {
    private Optional<Integer> maxSize;
    private int size;
    private ByteArrayOutputStream binaryStream = new ByteArrayOutputStream();

    public MessageBuilder(int maxSize) {
      this(Optional.of(maxSize));
    }

    public MessageBuilder() {
      this(Optional.empty());
    }

    public MessageBuilder(Optional<Integer> maxSize) {
      this.maxSize = maxSize;

      try {
        binaryStream.write(MAGIC_ID_BYTES);
        size += MAGIC_ID_BYTES.length;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public int getSize() {
      return size;
    }

    public boolean addMessage(EventuateKafkaMultiMessageKeyValue message) {
      try {
        byte[] keyBytes = Optional.ofNullable(message.getKey()).map(String::getBytes).orElse(new byte[0]);
        byte[] valueBytes = Optional.ofNullable(message.getValue()).map(String::getBytes).orElse(new byte[0]);

        int additionalSize = 2 * 4 + keyBytes.length + valueBytes.length;

        if (maxSize.map(ms -> size + additionalSize > ms).orElse(false)) {
          return false;
        }

        binaryStream.write(intToBytes(keyBytes.length));
        binaryStream.write(keyBytes);
        binaryStream.write(intToBytes(valueBytes.length));
        binaryStream.write(valueBytes);

        size += additionalSize;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      return true;
    }

    public byte[] toBinaryArray() {
      return binaryStream.toByteArray();
    }
  }
}
