package io.eventuate.messaging.kafka.common;

import org.apache.commons.lang.builder.EqualsBuilder;

import java.util.Objects;

public class EventuateKafkaMultiMessageKeyValue {
  private String key;
  private String value;

  public EventuateKafkaMultiMessageKeyValue(String key, String value) {
    this.key = key;
    this.value = value;
  }

  public String getKey() {
    return key;
  }

  public String getValue() {
    return value;
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
