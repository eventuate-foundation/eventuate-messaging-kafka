package io.eventuate.messaging.kafka.common;

import io.eventuate.messaging.kafka.common.sbe.MultiMessageEncoder;

import java.util.Collections;
import java.util.List;

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
  public int estimateSize() {
    int headerSize = MultiMessageEncoder.MessagesEncoder.HeadersEncoder.HEADER_SIZE;
    int messagesSize = headers.stream().map(KeyValue::estimateSize).reduce(0, (a, b) -> a + b);
    return super.estimateSize() + headerSize + messagesSize;
  }
}
