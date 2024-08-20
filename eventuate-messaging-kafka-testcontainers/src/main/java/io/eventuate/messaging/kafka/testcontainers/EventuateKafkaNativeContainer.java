package io.eventuate.messaging.kafka.testcontainers;


import com.github.dockerjava.api.command.InspectContainerResponse;
import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import org.jetbrains.annotations.NotNull;
import org.testcontainers.kafka.KafkaContainer;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class EventuateKafkaNativeContainer extends KafkaContainer implements PropertyProvidingContainer {

    private String firstNetworkAlias;

    private static final String STARTER_SCRIPT = "/tmp/testcontainers_start.sh";

    public EventuateKafkaNativeContainer() {
        super("apache/kafka-native");
        withCommand("sh", "-c", "id && while [ ! -f " + STARTER_SCRIPT + " ]; do sleep 0.1; done; " + STARTER_SCRIPT);

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

    @Override
    protected void containerIsStarting(InspectContainerResponse containerInfo) {
        try {
            ExecResult result = execInContainer("ls", "-ltd", "/opt/kafka/config/");
            printExecResult(result, "ls -ltd /opt/kafka/config/: ");
            result = execInContainer("id");
            printExecResult(result, "id");
            result = execInContainer("touch", "/opt/kafka/config/foo");
            printExecResult(result, "touch /opt/kafka/config/foo");
            result = execInContainer("bash", "-c", "[[ -w /opt/kafka/config/ ]] && echo yes");
            printExecResult(result, "bash -c '[[ -w /opt/kafka/config/ ]] && echo yes'");
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        super.containerIsStarting(containerInfo);
    }

    private static void printExecResult(ExecResult result, String command) {
        System.out.println(command + result.getExitCode());
        System.out.println(command + result.getStdout());
        System.out.println(command + result.getStderr());
    }
}
