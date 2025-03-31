package it.obenland.lib.eventest;

import static it.obenland.lib.eventtest.EventAsserter.*;
import static org.assertj.core.api.Assertions.*;

import it.obenland.lib.eventtest.EventAsserter;
import org.junit.jupiter.api.Test;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class AssertTests extends AbstractTests {
  @Container @ServiceConnection
  static final ConfluentKafkaContainer kafkaContainer =
      new ConfluentKafkaContainer(DockerImageName.parse(KAFKA_IMAGE));

  @Test
  void assertEventIsProduced() {
    sync(sendTestEvent());

    var events = assertEvent().isProduced();

    assertThat(events).hasSize(1)
        .first()
        .satisfies(this::assertIsTestEvent);
  }

  @Test
  void assertEventIsCommitted() {
    var sendResult= sendTestEvent();
    var assertion = assertEvent();

    assertThatThrownBy(assertion::isCommitted).isInstanceOf(AssertionError.class);

    sync(sendResult);
    var events = assertEvent().isCommitted();
    assertThat(events).hasSize(1)
        .first()
        .satisfies(this::assertIsTestEvent);
  }

  @Test
  void assertEventIsConsumed() {
    assertThatThrownBy(() -> assertEvent().isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("❌\tFound no records");
    sync(sendTestEvent());
    var events = assertEvent().isConsumed();
    assertThat(events).hasSize(1)
        .first()
        .satisfies(this::assertIsTestEvent);
  }

  @Test
  void assertEventWithTopicIsCommitted() {
    sync(sendTestEvent());
    assertEvent().withTopic("test.topic").isCommitted();

    assertThatThrownBy(() -> assertEvent().withTopic("test.other.topic").isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with topic 'test.other.topic'
            \tFound records with topics:
            \t🚫\ttest.topic\
            """);
  }

  @Test
  void assertEventWithTopicIsConsumed() {
    assertThatThrownBy(() -> assertEvent().withTopic("test.topic").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound no records with topic 'test.topic'\
            """);

    sync(sendTestEvent());
    assertEvent().withTopic("test.topic").isConsumed();

    assertThatThrownBy(() -> assertEvent().withTopic("test.other.topic").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with topic 'test.other.topic'
            \tFound records with topics:
            \t🚫\ttest.topic\
            """);
  }

  @Test
  void assertEventWithTopicIsProduced() {
    sync(sendTestEvent());
    assertEvent().withTopic("test.topic").isProduced();

    assertThatThrownBy(() -> assertEvent().withTopic("test.other.topic").isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with topic 'test.other.topic'
            \tFound records with topics:
            \t🚫\ttest.topic\
            """);
  }

  @Test
  void assertEventWithKeyIsCommitted() {
    sync(sendTestEvent());
    assertEvent().withKey("test.key").isCommitted();

    assertThatThrownBy(() -> assertEvent().withKey("test.other.key").isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with key 'test.other.key'
            \tFound records with keys:
            \t🚫\ttest.key\
            """);

    assertThatCode(() -> assertEvent().withKey("test.key").isCommitted())
        .doesNotThrowAnyException();
  }

  @Test
  void assertEventWithKeyIsConsumed() {
    assertThatThrownBy(() -> assertEvent().withKey("test.key").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound no records with key 'test.key'\
            """);
    sync(sendTestEvent());
    assertEvent().withKey("test.key").isConsumed();

    assertThatThrownBy(() -> assertEvent().withKey("test.other.key").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with key 'test.other.key'
            \tFound records with keys:
            \t🚫\ttest.key\
            """);

    assertThatCode(() -> assertEvent().withKey("test.key").isConsumed()).doesNotThrowAnyException();
  }

  @Test
  void assertEventWithKeyIsProduced() {
    sync(sendTestEvent());
    assertEvent().withKey("test.key").isProduced();

    assertThatThrownBy(() -> assertEvent().withKey("test.other.key").isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with key 'test.other.key'
            \tFound records with keys:
            \t🚫\ttest.key\
            """);

    assertThatCode(() -> assertEvent().withKey("test.key").isProduced()).doesNotThrowAnyException();
  }

  @Test
  void assertEventWithContentTypeIsCommitted() {
    sync(sendTestEvent());
    assertEvent().withContentType("test.contentType").isCommitted();

    assertThatThrownBy(() -> assertEvent().withContentType("test.other.contentType").isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with content type 'test.other.contentType'
            \tFound records with content types:
            \t🚫\ttest.contentType\
            """);
  }

  @Test
  void assertEventWithContentTypeIsConsumed() {
    assertThatThrownBy(() -> assertEvent().withContentType("test.contentType").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound no records with content type 'test.contentType'\
            """);

    sync(sendTestEvent());
    assertEvent().withContentType("test.contentType").isConsumed();

    assertThatThrownBy(() -> assertEvent().withContentType("test.other.contentType").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with content type 'test.other.contentType'
            \tFound records with content types:
            \t🚫\ttest.contentType\
            """);
  }

  @Test
  void assertEventWithContentTypeIsProduced() {
    sync(sendTestEvent());
    assertEvent().withContentType("test.contentType").isProduced();

    assertThatThrownBy(() -> assertEvent().withContentType("test.other.contentType").isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with content type 'test.other.contentType'
            \tFound records with content types:
            \t🚫\ttest.contentType\
            """);
  }

  @Test
  void assertEventWithHeaderIsCommitted() {
    sync(sendTestEvent());
    assertEvent().withHeader("test").isCommitted();

    assertThatThrownBy(() -> assertEvent().withHeader("test.other").isCommitted())
        .isInstanceOf(AssertionError.class)
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with 'test.other'-header
            \tFound records with headers:
            \t🚫\tContent-ID
            \t\tContent-Type
            \t\ttest\
            """);
  }

  @Test
  void assertEventWithHeaderIsConsumed() {
    assertThatThrownBy(() -> assertEvent().withHeader("test").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound no records with 'test'-header\
            """);

    sync(sendTestEvent());
    assertEvent().withHeader("test").isConsumed();

    assertThatThrownBy(() -> assertEvent().withHeader("test.other").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with 'test.other'-header
            \tFound records with headers:
            \t🚫\tContent-ID
            \t\tContent-Type
            \t\ttest\
            """);
  }

  @Test
  void assertEventWithHeaderIsProduced() {
    sync(sendTestEvent());
    assertEvent().withHeader("test").isProduced();

    assertThatThrownBy(() -> assertEvent().withHeader("test.other").isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with 'test.other'-header
            \tFound records with headers:
            \t🚫\tContent-ID
            \t\tContent-Type
            \t\ttest\
            """);
  }

  @Test
  void assertEventWithHeaderIsCommitted_string() {
    sync(sendTestEvent());
    assertEvent().withHeader("test", "test.header").isCommitted();

    assertThatThrownBy(() -> assertEvent().withHeader("test", "test.other.header").isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with 'test'-header with value 'test.other.header'
            \tFound records with 'test'-headers:
            \t🚫\ttest.header
            \t\ttest.header.2\
            """);
  }

  @Test
  void assertEventWithHeaderIsConsumed_string() {
    assertThatThrownBy(() -> assertEvent().withHeader("test", "test.header").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound no records with 'test'-header with value 'test.header'\
            """);
    sync(sendTestEvent());
    assertEvent().withHeader("test", "test.header").isConsumed();

    assertThatThrownBy(() -> assertEvent().withHeader("test", "test.other.header").isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with 'test'-header with value 'test.other.header'
            \tFound records with 'test'-headers:
            \t🚫\ttest.header
            \t\ttest.header.2\
            """);
  }

  @Test
  void assertEventWithHeaderIsProduced_string() {
    sync(sendTestEvent());
    assertEvent().withHeader("test", "test.header").isProduced();

    assertThatThrownBy(() -> assertEvent().withHeader("test", "test.other.header").isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound no records with 'test'-header with value 'test.other.header'
            \tFound records with 'test'-headers:
            \t🚫\ttest.header
            \t\ttest.header.2\
            """);
  }

  @Test
  void assertEventWithHeaderIsCommitted_byteArray() {
    sync(sendTestEvent());
    assertEvent().withHeader("test", "test.header".getBytes()).isCommitted();

    assertThatThrownBy(
            () -> assertEvent().withHeader("test", "test.other.header".getBytes()).isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
☑️\tFound 1 records
❌\tFound no records with 'test'-header with value '[116, 101, 115, 116, 46, 111, 116, 104, 101, 114, 46, 104, 101, 97, 100, 101, 114]'
\tFound records with 'test'-headers:
\t🚫\t[116, 101, 115, 116, 46, 104, 101, 97, 100, 101, 114, 46, 50]
\t\t[116, 101, 115, 116, 46, 104, 101, 97, 100, 101, 114]\
""");
  }

  @Test
  void assertEventWithHeaderIsConsumed_byteArray() {
    assertThatThrownBy(
            () -> assertEvent().withHeader("test", "test.header".getBytes()).isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
❌\tFound no records
❌\tFound no records with 'test'-header with value '[116, 101, 115, 116, 46, 104, 101, 97, 100, 101, 114]'\
""");

    sync(sendTestEvent());
    assertEvent().withHeader("test", "test.header".getBytes()).isConsumed();

    assertThatThrownBy(
            () -> assertEvent().withHeader("test", "test.other.header".getBytes()).isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
☑️\tFound 1 records
❌\tFound no records with 'test'-header with value '[116, 101, 115, 116, 46, 111, 116, 104, 101, 114, 46, 104, 101, 97, 100, 101, 114]'
\tFound records with 'test'-headers:
\t🚫\t[116, 101, 115, 116, 46, 104, 101, 97, 100, 101, 114, 46, 50]
\t\t[116, 101, 115, 116, 46, 104, 101, 97, 100, 101, 114]\
""");
  }

  @Test
  void assertEventWithHeaderIsProduced_byteArray() {
    sync(sendTestEvent());
    assertEvent().withHeader("test", "test.header".getBytes()).isProduced();

    assertThatThrownBy(
            () -> assertEvent().withHeader("test", "test.other.header".getBytes()).isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
☑️\tFound 1 records
❌\tFound no records with 'test'-header with value '[116, 101, 115, 116, 46, 111, 116, 104, 101, 114, 46, 104, 101, 97, 100, 101, 114]'
\tFound records with 'test'-headers:
\t🚫\t[116, 101, 115, 116, 46, 104, 101, 97, 100, 101, 114, 46, 50]
\t\t[116, 101, 115, 116, 46, 104, 101, 97, 100, 101, 114]\
""");
  }

  @Test
  void assertEventTimesIsCommittes() {
    assertThatThrownBy(() -> assertEvent().times(2).isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound 0 records but expected 2\
            """);

    sync(sendTestEvent());
    assertThatThrownBy(() -> assertEvent().times(2).isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound 1 records but expected 2\
            """);

    sync(sendTestEvent());
    assertEvent().times(2).isCommitted();
  }

  @Test
  void assertEventTimesIsConsumed() {
    assertThatThrownBy(() -> assertEvent().times(2).isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound 0 records but expected 2\
            """);

    sync(sendTestEvent());
    assertThatThrownBy(() -> assertEvent().times(2).isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound 1 records but expected 2\
            """);

    sync(sendTestEvent());
    assertEvent().times(2).isConsumed();
  }

  @Test
  void assertEventTimesIsProduced() {
    assertThatThrownBy(() -> assertEvent().times(2).isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound 0 records but expected 2\
            """);

    sync(sendTestEvent());
    assertThatThrownBy(() -> assertEvent().times(2).isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound 1 records but expected 2\
            """);

    sync(sendTestEvent());
    assertEvent().times(2).isProduced();
  }

  @Test
  void assertEventOneIsCommitted() {
    assertThatThrownBy(() -> assertEvent().one().isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound 0 records but expected one\
            """);
    sync(sendTestEvent());
    assertEvent().one().isCommitted();

    sync(sendTestEvent());
    assertThatThrownBy(() -> assertEvent().one().isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 2 records
            ❌\tFound 2 records but expected one\
            """);
  }

  @Test
  void assertEventOneIsConsumed() {
    assertThatThrownBy(() -> assertEvent().one().isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound 0 records but expected one\
            """);
    sync(sendTestEvent());
    assertEvent().one().isConsumed();

    sync(sendTestEvent());
    assertThatThrownBy(() -> assertEvent().one().isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 2 records
            ❌\tFound 2 records but expected one\
            """);
  }

  @Test
  void assertEventOneIsProduced() {
    assertThatThrownBy(() -> assertEvent().one().isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound no records
            ❌\tFound 0 records but expected one\
            """);
    sync(sendTestEvent());
    assertEvent().one().isProduced();

    sync(sendTestEvent());
    assertThatThrownBy(() -> assertEvent().one().isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 2 records
            ❌\tFound 2 records but expected one\
            """);
  }

  @Test
  void assertEventNoneIsCommitted() {
    assertEvent().none().isCommitted();
    sync(sendTestEvent());
    assertThatThrownBy(() -> assertEvent().none().isCommitted())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound 1 records but expected none\
            """);
  }

  @Test
  void assertEventNoneIsConsumed() {
    assertEvent().none().isConsumed();
    sync(sendTestEvent());
    assertThatThrownBy(() -> assertEvent().none().isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound 1 records but expected none\
            """);
  }

  @Test
  void assertEventNoneIsProduced() {
    assertEvent().none().isProduced();
    sync(sendTestEvent());
    assertThatThrownBy(() -> assertEvent().none().isProduced())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ❌\tFound 1 records but expected none\
            """);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  void validate() {
    assertEvent().withTopic("test.topic").withKey("test.key").withContentType("test.contentType");
    awaitEvent()
        .withTopic("test.other.topic")
        .withKey("test.other.key")
        .withContentType("test.other.contentType")
        .times(10);
    assertThatThrownBy(EventAsserter::validate)
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            Found unfinished assertions:
            ❌\tassertEvent()
            \t\t.withTopic("test.topic")
            \t\t.withKey("test.key")
            \t\t.withContentType("test.contentType")
            ❌\tawaitEvent()
            \t\t.withTopic("test.other.topic")
            \t\t.withKey("test.other.key")
            \t\t.withContentType("test.other.contentType")
            \t\t.times(10)
            """);
  }
}
