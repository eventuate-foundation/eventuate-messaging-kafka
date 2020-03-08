package io.eventuate.messaging.kafka.basic.consumer;

public class BackPressureConfig {

  private int low = 0;
  private int high = Integer.MAX_VALUE;

  public BackPressureConfig() {
  }

  public BackPressureConfig(int low, int high) {
    this.low = low;
    this.high = high;
  }

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
}
