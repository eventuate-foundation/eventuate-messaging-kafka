package io.eventuate.messaging.kafka.consumer;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public interface ReactiveKafkaMessageHandler extends Function<KafkaMessage, CompletableFuture<Void>> {

}
