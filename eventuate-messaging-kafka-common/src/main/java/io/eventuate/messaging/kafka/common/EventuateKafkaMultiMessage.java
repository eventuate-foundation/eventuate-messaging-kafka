package io.eventuate.messaging.kafka.common;

import io.eventuate.messaging.kafka.common.sbe.MultiMessageEncoder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class EventuateKafkaMultiMessage extends KeyValue {

  private List<EventuateKafkaMultiMessageHeader> headers;

  public EventuateKafkaMultiMessage(String key, String value) {
    this(key, value, Collections.emptyList());
  }

  public EventuateKafkaMultiMessage(String key, String value, List<EventuateKafkaMultiMessageHeader> headers) {
    super(key, value);
    this.headers = headers;
  }


  public List<EventuateKafkaMultiMessageHeader> getHeaders() {
    return headers;
  }

  @Override
  public boolean equals(Object o) {
    return EqualsBuilder.reflectionEquals(this, o);
  }

  @Override
  public int hashCode() {
    return HashCodeBuilder.reflectionHashCode(this);
  }

  @Override
  public int estimateSize() {
    int headerSize = MultiMessageEncoder.MessagesEncoder.HeadersEncoder.HEADER_SIZE;
    int messagesSize = KeyValue.estimateSize(headers);
    return super.estimateSize() + headerSize + messagesSize;
  }


}
