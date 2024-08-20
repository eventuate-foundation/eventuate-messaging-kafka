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
        execInContainerAndPrint("ls", "-ltd", "/opt/kafka/config/");
        execInContainerAndPrint("id");
        execInContainerAndPrint("touch", "/opt/kafka/config/foo");

        String writeableTest = "[[ -w /opt/kafka/config/ ]] && echo yes";
        execInContainerAndPrint("sh", "-c", writeableTest);
        execInContainerAndPrint("bash", "-c", writeableTest);

        super.containerIsStarting(containerInfo);
    }

    private void execInContainerAndPrint(String... args)  {
        try {
            ExecResult result = execInContainer(args);
            String command = String.join(" ", args) + " : ";
            System.out.println("======== Command " + command + " -> ");
            System.out.println("ExitCode=" + result.getExitCode());
            System.out.println("Stdout=" + result.getStdout());
            System.out.println("Stderr=" + result.getStderr());
        } catch (UnsupportedOperationException | InterruptedException | IOException e) {
            throw new RuntimeException(e);
        }
    }

}
