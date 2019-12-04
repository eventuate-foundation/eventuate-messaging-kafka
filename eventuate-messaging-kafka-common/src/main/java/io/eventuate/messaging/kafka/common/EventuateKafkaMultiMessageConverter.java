package io.eventuate.messaging.kafka.common;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class EventuateKafkaMultiMessageConverter {
  public static final String MAGIC_ID = "a8c79db675e14c4cbf1eb77d0d6d0f00"; // generated UUID
  public static final byte[] MAGIC_ID_BYTES = MAGIC_ID.getBytes();

  public byte[] convertMessagesToBytes(List<EventuateKafkaMultiMessageKeyValue> messages) {
    ByteArrayOutputStream binaryStream = new ByteArrayOutputStream();

    try {
      binaryStream.write(MAGIC_ID_BYTES);

      for (EventuateKafkaMultiMessageKeyValue message : messages) {
        byte[] keyBytes = Optional.ofNullable(message.getKey()).map(String::getBytes).orElse(new byte[0]);
        binaryStream.write(intToBytes(keyBytes.length));
        binaryStream.write(keyBytes);

        byte[] valueBytes =Optional.ofNullable(message.getValue()).map(String::getBytes).orElse(new byte[0]);
        binaryStream.write(intToBytes(valueBytes.length));
        binaryStream.write(valueBytes);
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return binaryStream.toByteArray();
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
          key = new String(keyBytes);
        }

        int valueLength = readInt(binaryStream);
        if (valueLength > 0) {
          byte[] valueBytes = new byte[valueLength];
          binaryStream.read(valueBytes);
          value = new String(valueBytes);
        }

        messages.add(new EventuateKafkaMultiMessageKeyValue(key, value));
      }

      return messages;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private byte[] intToBytes(int value) {
    return ByteBuffer.allocate(4).putInt(value).array();
  }

  private int bytesToInt(byte[] bytes) {
    return ByteBuffer.wrap(bytes).getInt();
  }

  private int readInt(ByteArrayInputStream binaryStream) throws IOException {
    byte[] buf = new byte[4];
    binaryStream.read(buf);
    return bytesToInt(buf);
  }

}
