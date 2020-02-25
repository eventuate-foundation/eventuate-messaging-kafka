package io.eventuate.messaging.kafka.micronaut.basic.consumer;

import io.eventuate.messaging.kafka.basic.consumer.BackPressureConfig;
import io.micronaut.context.annotation.ConfigurationProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

@ConfigurationProperties("eventuate.local.kafka.consumer")
public class EventuateKafkaConsumerMicronautConfigurationProperties {
  Map<String, String> properties = new HashMap<>();

  private BackPressureProperties backPressure = new BackPressureProperties();
  private long pollTimeout = 100;

  public BackPressureProperties getBackPressure() {
    return backPressure;
  }

  public void setBackPressure(BackPressureProperties backPressure) {
    this.backPressure = backPressure;
  }

  public long getPollTimeout() {
    return pollTimeout;
  }

  public void setPollTimeout(long pollTimeout) {
    this.pollTimeout = pollTimeout;
  }
  public Map<String, String> getProperties() {
    return properties
            .entrySet()
            .stream()
            .collect(Collectors.toMap(o -> o.getKey().replace("-", "."), Map.Entry::getValue));
  }

  @ConfigurationProperties("backPressure")
  public static class BackPressureProperties {
    private int low = 0;
    private int high = Integer.MAX_VALUE;

    public int getLow() {
      return low;
    }

    public void setLow(int low) {
      this.low = low;
    }

    public int getHigh() {
      return high;
    }

    public void setHigh(int high) {
      this.high = high;
    }

    public BackPressureConfig toBackPressureConfig() {
      return new BackPressureConfig(low, high);
    }
  }
}
