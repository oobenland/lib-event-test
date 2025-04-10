package it.obenland.lib.eventest;

import static it.obenland.lib.eventtest.EventAsserter.awaitEvent;
import static it.obenland.lib.eventtest.EventAsserter.sync;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import it.obenland.lib.eventtest.EventPayload;
import java.util.HashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
class AwaitTests extends AbstractTests {
  @Container @ServiceConnection
  static final ConfluentKafkaContainer kafkaContainer =
      new ConfluentKafkaContainer(DockerImageName.parse(KAFKA_IMAGE));

  @Autowired private ProducerFactory<String, String> producerFactory;

  @BeforeAll
  static void beforeAll() {
    Awaitility.setDefaultTimeout(300, TimeUnit.MILLISECONDS);
  }

  @AfterAll
  static void afterAll() {
    Awaitility.reset();
  }

  @Test
  void awaitEventIsProduced() {
    assertThatThrownBy(() -> awaitEvent().isProduced())
        .isInstanceOf(ConditionTimeoutException.class);
    sendTestEvent();
    var events = awaitEvent().isProduced();
    assertThat(events).hasSize(1).first().satisfies(this::assertIsTestEvent);
  }

  @Test
  void awaitEventIsCommitted() {
    assertThatThrownBy(() -> awaitEvent().isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);
    sendTestEvent();
    var events = awaitEvent().isCommitted();
    assertThat(events).hasSize(1).first().satisfies(this::assertIsTestEvent);
  }

  @Test
  void awaitEventIsConsumed() {
    assertThatThrownBy(() -> awaitEvent().isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);
    sendTestEvent();
    var events = awaitEvent().isConsumed();
    assertThat(events).hasSize(1).first().satisfies(this::assertIsTestEvent);
  }

  @Test
  void awaitEventWithTopicIsCommitted() {
    sendTestEvent();
    awaitEvent().withTopic("test.topic").isCommitted();

    assertThatThrownBy(() -> awaitEvent().withTopic("test.other.topic").isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithTopicIsConsumed() {
    sendTestEvent();
    awaitEvent().withTopic("test.topic").isConsumed();

    assertThatThrownBy(() -> awaitEvent().withTopic("test.other.topic").isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithTopicIsProduced() {
    sendTestEvent();
    awaitEvent().withTopic("test.topic").isProduced();

    assertThatThrownBy(() -> awaitEvent().withTopic("test.other.topic").isProduced())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithKeyIsCommitted() {
    sendTestEvent();
    awaitEvent().withKey("test.key").isCommitted();

    assertThatThrownBy(() -> awaitEvent().withKey("test.other.key").isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithKeyIsConsumed() {
    sendTestEvent();
    awaitEvent().withKey("test.key").isConsumed();

    assertThatThrownBy(() -> awaitEvent().withKey("test.other.key").isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithKeyIsProduced() {
    sendTestEvent();
    awaitEvent().withKey("test.key").isProduced();

    assertThatThrownBy(() -> awaitEvent().withKey("test.other.key").isProduced())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithContentTypeIsCommitted() {
    sendTestEvent();
    awaitEvent().withContentType("test.contentType").isCommitted();

    assertThatThrownBy(() -> awaitEvent().withContentType("test.other.contentType").isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithContentTypeIsConsumed() {
    sendTestEvent();
    awaitEvent().withContentType("test.contentType").isConsumed();

    assertThatThrownBy(() -> awaitEvent().withContentType("test.other.contentType").isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithContentTypeIsProduced() {
    sendTestEvent();
    awaitEvent().withContentType("test.contentType").isProduced();

    assertThatThrownBy(() -> awaitEvent().withContentType("test.other.contentType").isProduced())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithHeaderIsCommitted() {
    sendTestEvent();
    awaitEvent().withHeader("test").isCommitted();

    assertThatThrownBy(() -> awaitEvent().withHeader("test.other").isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithHeaderIsConsumed() {
    sendTestEvent();
    awaitEvent().withHeader("test").isConsumed();

    assertThatThrownBy(() -> awaitEvent().withHeader("test.other").isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithHeaderIsProduced() {
    sendTestEvent();
    awaitEvent().withHeader("test").isProduced();

    assertThatThrownBy(() -> awaitEvent().withHeader("test.other").isProduced())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithHeaderIsCommitted_string() {
    sendTestEvent();
    awaitEvent().withHeader("test", "test.header").isCommitted();

    assertThatThrownBy(() -> awaitEvent().withHeader("test", "test.other.header").isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithHeaderIsConsumed_string() {
    sendTestEvent();
    awaitEvent().withHeader("test", "test.header").isConsumed();

    assertThatThrownBy(() -> awaitEvent().withHeader("test", "test.other.header").isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithHeaderIsProduced_string() {
    sendTestEvent();
    awaitEvent().withHeader("test", "test.header").isProduced();

    assertThatThrownBy(() -> awaitEvent().withHeader("test", "test.other.header").isProduced())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithHeaderIsCommitted_byteArray() {
    sendTestEvent();
    awaitEvent().withHeader("test", "test.header".getBytes()).isCommitted();

    assertThatThrownBy(() -> awaitEvent().withHeader("test", "test.other.header").isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithHeaderIsConsumed_byteArray() {
    sendTestEvent();
    awaitEvent().withHeader("test", "test.header".getBytes()).isConsumed();

    assertThatThrownBy(() -> awaitEvent().withHeader("test", "test.other.header").isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventWithHeaderIsProduced_byteArray() {
    sendTestEvent();
    awaitEvent().withHeader("test", "test.header".getBytes()).isProduced();

    assertThatThrownBy(() -> awaitEvent().withHeader("test", "test.other.header").isProduced())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventTimesIsCommittes() {
    assertThatThrownBy(() -> awaitEvent().times(2).isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);

    sync(sendTestEvent());
    assertThatThrownBy(() -> awaitEvent().times(2).isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);

    sync(sendTestEvent());
    awaitEvent().times(2).isCommitted();
  }

  @Test
  void awaitEventTimesIsConsumed() {
    assertThatThrownBy(() -> awaitEvent().times(2).isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);

    sync(sendTestEvent());
    assertThatThrownBy(() -> awaitEvent().times(2).isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);

    sync(sendTestEvent());
    awaitEvent().times(2).isConsumed();
  }

  @Test
  void awaitEventTimesIsProduced() {
    assertThatThrownBy(() -> awaitEvent().times(2).isProduced())
        .isInstanceOf(ConditionTimeoutException.class);

    sync(sendTestEvent());
    assertThatThrownBy(() -> awaitEvent().times(2).isProduced())
        .isInstanceOf(ConditionTimeoutException.class);

    sync(sendTestEvent());
    awaitEvent().times(2).isProduced();
  }

  @Test
  void awaitEventOneIsCommittee() {
    assertThatThrownBy(() -> awaitEvent().one().isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);
    sync(sendTestEvent());
    awaitEvent().one().isCommitted();
  }

  @Test
  void awaitEventOneIsConsumed() {
    assertThatThrownBy(() -> awaitEvent().one().isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);
    sendTestEvent();
    awaitEvent().one().isConsumed();
  }

  @Test
  void awaitEventOneIsProduced() {
    assertThatThrownBy(() -> awaitEvent().one().isProduced())
        .isInstanceOf(ConditionTimeoutException.class);
    sync(sendTestEvent());
    awaitEvent().one().isProduced();
  }

  @Test
  void awaitEventNoneIsCommittee() {
    awaitEvent().none().isCommitted();
    sync(sendTestEvent());
    assertThatThrownBy(() -> awaitEvent().none().isCommitted())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventNoneIsConsumed() {
    awaitEvent().none().isConsumed();
    sendTestEvent();
    assertThatThrownBy(() -> awaitEvent().none().isConsumed())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void awaitEventNoneIsProduced() {
    awaitEvent().none().isProduced();
    sync(sendTestEvent());
    assertThatThrownBy(() -> awaitEvent().none().isProduced())
        .isInstanceOf(ConditionTimeoutException.class);
  }

  @Test
  void canProcessByteArrayEvents() throws ExecutionException, InterruptedException {
    var configProps = new HashMap<>(producerFactory.getConfigurationProperties());
    configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
    var byteArrayKafkaTemplate =
        new KafkaTemplate<>(new DefaultKafkaProducerFactory<byte[], byte[]>(configProps));

    var serializer = new StringSerializer();

    byteArrayKafkaTemplate
        .send(
            "test.topic",
            serializer.serialize("test.topic", "test.key"),
            serializer.serialize(
                "test.topic",
                """
                {
                  "id": 1
                }
                """))
        .get();

    awaitEvent()
        .withTopic("test.topic")
        .withKey("test.key")
        .withPayload(
            EventPayload.fromJson(
                """
                {
                          "id": 1
                }
                """))
        .isProduced();
  }
}
