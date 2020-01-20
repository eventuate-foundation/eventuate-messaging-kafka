package io.eventuate.messaging.kafka.common;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.Objects;

public class KeyValue {
  private String key;
  private String value;

  public KeyValue(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
  }

  public int estimateSize() {
    int keyLength = estimatedStringSizeInBytes(key);
    int valueLength = estimatedStringSizeInBytes(value);
    return 2 * 4 + keyLength + valueLength;
  }

  private int estimatedStringSizeInBytes(String s) {
    return s == null ? 0 : s.length() * 2;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this);
  }

  @Override
  public boolean equals(Object o) {
    return EqualsBuilder.reflectionEquals(this, o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(key, value);
  }
}
