package io.eventuate.messaging.kafka.testcontainers;

import com.github.dockerjava.api.command.InspectContainerResponse;
import io.eventuate.common.testcontainers.ContainerUtil;
import io.eventuate.common.testcontainers.EventuateGenericContainer;
import io.eventuate.common.testcontainers.PropertyProvidingContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import java.io.IOException;
import java.nio.file.Path;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class EventuateKafkaContainer extends EventuateGenericContainer<EventuateKafkaContainer> implements PropertyProvidingContainer {

    private final String zookeeperConnect;

    public EventuateKafkaContainer(String zookeeperConnect) {
        super(ContainerUtil.findImage("eventuateio/eventuate-kafka", "eventuate.messaging.kafka.version.properties"));
        this.zookeeperConnect = zookeeperConnect;
        withConfiguration();
    }

    @Override
    protected int getPort() {
        return 29092;
    }

    public EventuateKafkaContainer(Path path, String zookeeperConnect) {
        super(new ImageFromDockerfile().withDockerfile(path));
        this.zookeeperConnect = zookeeperConnect;
        withConfiguration();
    }

    private void withConfiguration() {
        withEnv("KAFKA_LISTENERS", "LC://kafka:29092,LX://kafka:9092");
        withEnv("KAFKA_ADVERTISED_LISTENERS", "LC://kafka:29092,LX://localhost:9092");
        withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "LC:PLAINTEXT,LX:PLAINTEXT");
        withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "LC");
        withEnv("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1");
        withEnv("KAFKA_ZOOKEEPER_CONNECTION_TIMEOUT_MS", "60000");
        withEnv("KAFKA_ZOOKEEPER_CONNECT", zookeeperConnect);
        withExposedPorts(9092);
    }

    @Override
    public void start() {
        super.start();
    }

    @Override
    public void registerProperties(BiConsumer<String, Supplier<Object>> registry) {
        registry.accept("eventuatelocal.kafka.bootstrap.servers",
                () -> String.format("localhost:%s", getFirstMappedPort()));
    }

    protected void containerIsStarted(InspectContainerResponse containerInfo) {
        super.containerIsStarted(containerInfo);
        execute("bin/kafka-configs.sh", "--alter", "--bootstrap-server", "kafka:29092", "--entity-type", "brokers", "--entity-name", "0", "--add-config", String.format("advertised.listeners=[LX://localhost:%s,LC://kafka:29092]", getMappedPort(9092)));
    }

    private void execute(String... command) {
        ExecResult result;
        try {
            result = execInContainer(command);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (result.getExitCode() != 0) {
            throw new IllegalStateException(result.toString());
        }
    }
}
