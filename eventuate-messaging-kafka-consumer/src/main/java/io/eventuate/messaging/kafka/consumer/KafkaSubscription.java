package io.eventuate.messaging.kafka.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSubscription {
  private Logger logger = LoggerFactory.getLogger(this.getClass());

  private Runnable closingCallback;

  public KafkaSubscription(Runnable closingCallback) {
    this.closingCallback = closingCallback;
  }

  public void close() {
    logger.info("Closing kafka subscription");
    closingCallback.run();
    logger.info("Closed kafka subscription");
  }
}
