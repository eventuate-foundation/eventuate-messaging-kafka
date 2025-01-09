package io.eventuate.messaging.kafka.testcontainers;


import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.kafka.KafkaContainer;

import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class EventuateKafkaNativeContainer extends KafkaContainer implements PropertyProvidingContainer {

    private String firstNetworkAlias;

    public EventuateKafkaNativeContainer() {
        super("apache/kafka-native:3.8.0");
    }

    public EventuateKafkaNativeContainer(String imageName) {
        super(imageName);
    }

    @Override
    protected void configure() {
        String controllerQuorumVoters = String.format("%s@%s:9094", getEnvMap().get("KAFKA_NODE_ID"), "localhost");
        withEnv("KAFKA_CONTROLLER_QUORUM_VOTERS", controllerQuorumVoters);
    }

    @Override
    public EventuateKafkaNativeContainer withNetworkAliases(String... aliases) {
        super.withNetworkAliases(aliases);
        this.firstNetworkAlias = aliases[0];
        return this;
    }

    @NotNull
    public String getBootstrapServersForContainer() {
        return  firstNetworkAlias + ":9093";
    }

    @Override
    public void registerProperties(BiConsumer<String, Supplier<Object>> registry) {
        registry.accept("eventuatelocal.kafka.bootstrap.servers", this::getBootstrapServers);
    }

}
