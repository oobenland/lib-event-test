package it.obenland.lib.eventest;

import static it.obenland.lib.eventtest.EventAsserter.assertEvent;
import static it.obenland.lib.eventtest.EventAsserter.sync;
import static it.obenland.lib.eventtest.EventPayload.fromFile;
import static it.obenland.lib.eventtest.EventPayload.fromJson;
import static org.assertj.core.api.Assertions.*;

import it.obenland.lib.TestPayloadExtensions;

import java.time.Instant;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
@ExtensionMethod(TestPayloadExtensions.class)
public class EventPayloadTests extends AbstractTests {
  @Container @ServiceConnection
  static final ConfluentKafkaContainer kafkaContainer =
      new ConfluentKafkaContainer(DockerImageName.parse(KAFKA_IMAGE));

  @Test
  void happyPathStrict() {
    sync(
        sendTestEvent(
            "test.topic",
            """
            {
              "id": "1234"
            }
            """));
    assertEvent()
        .withTopic("test.topic")
        .withPayload(
            fromJson(
                    """
                    {
                      "id": "${id}"
                    }
                    """)
                .withId("1234")
                .strict())
        .isConsumed();
  }

  @Test
  void unhappyPathStrict_arrayOrder() {
    sync(
        sendTestEvent(
            "test.topic",
            """
            {
              "id": "1234",
              "list": [
                "12345678",
                "987654321"
              ]
            }
            """));
    assertThatThrownBy(
            () ->
                assertEvent()
                    .withTopic("test.topic")
                    .withPayload(
                        fromJson(
                                """
                                {
                                  "id": "1234",
                                  "list": [
                                    "987654321",
                                    "12345678"
                                  ]
                                }
                                """)
                            .strict())
                    .isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 1 records
            ☑️\tFound 1 records with topic 'test.topic'
            ❌\tFound no records with eventPayload:
            \t{
            \t  "id" : "1234",
            \t  "list" : [ "987654321", "12345678" ]
            \t}
            \tFound records with payloads:
            \t🚫
            \t{
            \t  "id" : "1234",
            \t  "list" : [ "12345678", "987654321" ]
            \t}
            \tlist[0]
            \tExpected: 987654321
            \t     got: 12345678
            \t ; list[1]
            \tExpected: 12345678
            \t     got: 987654321\
            """);
  }

  @Test
  void happyPathLenient() {
    sync(
        sendTestEvent(
            "test.topic",
            """
            {
              "id": "1234",
              "key": "value"
            }
            """));
    assertEvent()
        .withTopic("test.topic")
        .withPayload(
            fromJson(
                    """
                    {
                      "id": "${id}"
                    }
                    """)
                .withId("1234")
                .lenient())
        .isConsumed();
  }

  @Test
  void missingField_fromJson() {
    sync(
        sendTestEvent(
            "test.topic",
            """
            {
              "id": "1234"
            }
            """));
    sync(
        sendTestEvent(
            "test.topic",
            """
            {
              "custom": "12345678"
            }
            """));
    assertThatThrownBy(
            () ->
                assertEvent()
                    .withTopic("test.topic")
                    .withPayload(
                        fromJson(
                                """
                                {
                                  "id": "${id}",
                                  "custom": "${custom}"
                                }
                                """)
                            .withId("1234")
                            .with("custom", "12345678")
                            .lenient())
                    .isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 2 records
            ☑️\tFound 2 records with topic 'test.topic'
            ❌\tFound no records with eventPayload:
            \t{
            \t  "id" : "1234",
            \t  "custom" : "12345678"
            \t}
            \tFound records with payloads:
            \t🚫
            \t{
            \t  "id" : "1234"
            \t}
            \tExpected: custom
            \t     but none found
            \t🚫
            \t{
            \t  "custom" : "12345678"
            \t}
            \tExpected: id
            \t     but none found\
            """);
  }

  @Test
  void missingField_fromFile() {
    sync(
        sendTestEvent(
            "test.topic",
            """
            {
              "id": "1234"
            }
            """));
    sync(
        sendTestEvent(
            "test.topic",
            """
            {
              "custom": "12345678"
            }
            """));
    assertThatThrownBy(
            () ->
                assertEvent()
                    .withTopic("test.topic")
                    .withPayload(
                        fromFile("/test-event.json")
                            .withId("1234")
                            .with("custom", "12345678")
                            .lenient())
                    .isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 2 records
            ☑️\tFound 2 records with topic 'test.topic'
            ❌\tFound no records with eventPayload:
            \t{
            \t  "id" : "1234",
            \t  "custom" : "12345678"
            \t}
            \tFound records with payloads:
            \t🚫
            \t{
            \t  "id" : "1234"
            \t}
            \tExpected: custom
            \t     but none found
            \t🚫
            \t{
            \t  "custom" : "12345678"
            \t}
            \tExpected: id
            \t     but none found\
            """);
  }

  @Test
  void placeholder() {
    Assertions.assertThat(
            fromJson(
                    """
                    {
                      "id": "${id}",
                      "timestamp": "${timestamp}",
                      "custom": "${custom}"
                    }
                    """)
                .withId("12345678")
                .withTimestamp(Instant.parse("2022-08-01T13:37:44.418949001Z"))
                .with("custom", "myCustomValue")
                .toString())
        .isEqualTo(
            """
            {
              "id": "12345678",
              "timestamp": "2022-08-01T13:37:44.418949001Z",
              "custom": "myCustomValue"
            }
            """);
  }

  @Test
  void placeholderNotFound() {
    assertThatThrownBy(() -> fromJson("{}").withId("12345678"))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("❌\tCan not find placeholder '${id}' in payload:\n{}");
  }

  @Test
  void placeholdersWereNotReplaced() {
    assertThatThrownBy(
        () ->
            fromJson(
                    """
                    {
                      "id": "${id}",
                      "key": "${value}",
                      "list": [
                        "${entry}"
                      ]
                    }\
                    """)
                .toString())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tNo placeholders are provided
            ☑️\tNo placeholders are ignored
            ❌\tFound unused placeholders:
            \t🚫\tid: "${id}"
            \t🚫\tkey: "${value}"
            \t🚫\tlist[0]: "${entry}"
            in payload:
            {
              "id": "${id}",
              "key": "${value}",
              "list": [
                "${entry}"
              ]
            }\
            """);
  }

  @Test
  void placeholdersWereNotReplaced_withGivenPlaceholders() {
    assertThatThrownBy(
        () ->
            fromJson(
                    """
                    {
                      "id": "${id}",
                      "key": "${key}",
                      "list": [
                        "my ${entry}"
                      ]
                    }\
                    """)
                .withId("12345678")
                .with("key", "myValue")
                .toString())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound provided placeholders:
            \t→\tid
            \t→\tkey
            ☑️\tNo placeholders are ignored
            ❌\tFound unused placeholders:
            \t🚫\tlist[0]: "my ${entry}"
            in payload:
            {
              "id": "12345678",
              "key": "myValue",
              "list": [
                "my ${entry}"
              ]
            }\
            """);
  }

  @Test
  void placeholdersWereNotReplaced_withIgnoredPlaceholders() {
    assertThatThrownBy(
            () ->
                fromJson("{\"id\": \"${id}\", \"key\": \"${value}\"}")
                    .ignorePlaceholders("id", "other")
                    .toString())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tNo placeholders are provided
            ☑️\tFound ignored placeholders:
            \t→\tid
            \t→\tother
            ❌\tFound unused placeholders:
            \t🚫\tkey: "${value}"
            in payload:
            {"id": "${id}", "key": "${value}"}\
            """);
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  void placeholdersWereNotReplaced_withIgnoredPlaceholders_all() {
    assertThatNoException()
        .isThrownBy(
            () ->
                fromJson("{\"id\": \"${id}\", \"key\": \"${value}\"}")
                    .ignorePlaceholders("id", "value")
                    .toString());
  }

  @SuppressWarnings("UnnecessaryToStringCall")
  @Test
  void invalidJson() {
    @Language("json")
    var json = new String(new byte[] {'{', 'a'});
    assertThatThrownBy(() -> log.info("{}", fromJson(json).toString()))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
❌	Invalid JSON provided. Unexpected character ('a' (code 97)): was expecting double-quote to start field name
 at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 2]:
{a\
""");
  }

  @Test
  void extensionMethod() {
    assertThat(fromJson("{\"custom\": \"${custom}\"}").withCustom("123").toString())
        .isEqualTo(
            fromJson("{\"custom\": \"${custom}\"}").with("custom", "123").toString());
  }
}
