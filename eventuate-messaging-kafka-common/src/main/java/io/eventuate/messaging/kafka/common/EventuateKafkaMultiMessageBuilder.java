package io.eventuate.messaging.kafka.common;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Optional;

public class EventuateKafkaMultiMessageBuilder {
  public static final String MAGIC_ID = "a8c79db675e14c4cbf1eb77d0d6d0f00"; // generated UUID
  public static final byte[] MAGIC_ID_BYTES = MAGIC_ID.getBytes();

  private int maxSize;
  private int size;
  private ByteArrayOutputStream binaryStream = new ByteArrayOutputStream();

  public EventuateKafkaMultiMessageBuilder(int maxSize) {
    this.maxSize = maxSize;

    try {
      binaryStream.write(MAGIC_ID_BYTES);
      size += MAGIC_ID_BYTES.length;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean addMessage(EventuateKafkaMultiMessageKeyValue message) {
    try {
      byte[] keyBytes = Optional.ofNullable(message.getKey()).map(String::getBytes).orElse(new byte[0]);
      byte[] valueBytes =Optional.ofNullable(message.getValue()).map(String::getBytes).orElse(new byte[0]);

      int additionalSize = 2 * 4 + keyBytes.length + valueBytes.length;

      if (size + additionalSize > maxSize) {
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
