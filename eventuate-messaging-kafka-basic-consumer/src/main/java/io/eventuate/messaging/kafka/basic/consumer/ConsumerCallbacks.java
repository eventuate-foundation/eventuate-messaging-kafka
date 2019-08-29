package io.eventuate.messaging.kafka.basic.consumer;

interface ConsumerCallbacks {
  void onTryCommitCallback();
  void onCommitedCallback();
  void onCommitFailedCallback();
}
