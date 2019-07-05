package io.eventuate.messaging.kafka.common;

public class EventuateKafkaConfigurationProperties {

  private String bootstrapServers;
  private long connectionValidationTimeout;

  public EventuateKafkaConfigurationProperties(String bootstrapServers, long connectionValidationTimeout) {
    this.bootstrapServers = bootstrapServers;
    this.connectionValidationTimeout = connectionValidationTimeout;
  }

  public String getBootstrapServers() {
    return bootstrapServers;
  }

  public long getConnectionValidationTimeout() {
    return connectionValidationTimeout;
  }
}
