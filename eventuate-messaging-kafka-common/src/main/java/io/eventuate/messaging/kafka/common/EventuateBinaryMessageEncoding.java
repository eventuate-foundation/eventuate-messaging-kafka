package io.eventuate.messaging.kafka.common;

import java.nio.charset.Charset;

public class EventuateBinaryMessageEncoding {
  public static String bytesToString(byte[] bytes) {
    return new String(bytes, Charset.forName("UTF-8"));
  }

  public static byte[] stringToBytes(String string) {
    return string.getBytes(Charset.forName("UTF-8"));
  }
}
