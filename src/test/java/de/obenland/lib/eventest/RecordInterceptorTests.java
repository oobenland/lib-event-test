package de.obenland.lib.eventest;

import de.obenland.lib.eventtest.EventAsserter;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
public class RecordInterceptorTests extends AbstractTests {

  @Container @ServiceConnection
  static final ConfluentKafkaContainer kafkaContainer =
      new ConfluentKafkaContainer(DockerImageName.parse(KAFKA_IMAGE));

  @Test
  void consumed() {
    EventAsserter.sync(sendTestEvent());
    EventAsserter.assertEvent().withTopic("test.topic").isConsumed();
  }

  @Test
  void nothingIsConsumed() {
    Assertions.assertThatThrownBy(() -> EventAsserter.assertEvent().withTopic("test.topic").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound no records with topic 'test.topic'\
            """);
    Assertions.assertThatThrownBy(() -> EventAsserter.assertEvent().isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("❌\tFound no records");
    EventAsserter.assertEvent().withTopic("test.topic").none().isConsumed();
  }

  @Test
  void committed() {
    EventAsserter.sync(sendTestEvent());
    EventAsserter.assertEvent().withTopic("test.topic").isCommitted();
  }

  @Test
  void nothingIsCommitted() {
    Assertions.assertThatThrownBy(
            () -> EventAsserter.assertEvent().withTopic("test.topic").isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound no records with topic 'test.topic'\
            """);
    Assertions.assertThatThrownBy(() -> EventAsserter.assertEvent().isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("❌\tFound no records");
    EventAsserter.assertEvent().withTopic("test.topic").none().isCommitted();
  }
}
