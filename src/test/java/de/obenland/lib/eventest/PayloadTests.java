package de.obenland.lib.eventest;

import static de.obenland.lib.eventtest.Asserter.assertEvent;
import static de.obenland.lib.eventtest.Asserter.sync;
import static org.assertj.core.api.Assertions.*;

import de.obenland.lib.TestPayloadExtensions;
import de.obenland.lib.eventtest.Payload;
import java.time.Instant;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.api.Assertions;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.Test;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Slf4j
@Testcontainers
@ExtensionMethod(TestPayloadExtensions.class)
public class PayloadTests extends AbstractTests {
  @Container @ServiceConnection
  static final KafkaContainer kafkaContainer =
      new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.2"));

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
            Payload.fromJson(
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
                        Payload.fromJson(
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
            ❌\tFound no records with payload:
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
            \t     got: 987654321""");
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
            Payload.fromJson(
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
                        Payload.fromJson(
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
            ❌\tFound no records with payload:
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
            \t     but none found""");
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
                        Payload.fromFile("/test-event.json")
                            .withId("1234")
                            .with("custom", "12345678")
                            .lenient())
                    .isConsumed())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ☑️\tFound 2 records
            ☑️\tFound 2 records with topic 'test.topic'
            ❌\tFound no records with payload:
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
            \t     but none found""");
  }

  @Test
  void placeholder() {
    Assertions.assertThat(
            Payload.fromJson(
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
    assertThatThrownBy(() -> Payload.fromJson("{}").withId("12345678"))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining("❌\tCan not find placeholder '${id}' in payload:\n{}");
  }

  @Test
  void placeholdersWereNotReplaced() {
    assertThatThrownBy(
            () ->
                Payload.fromJson(
                        """
                        {
                          "id": "${id}",
                          "key": "${value}",
                          "list": [
                            "${entry}"
                          ]
                        }""")
                    .toString())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound unused placeholders:
            \t🚫\tid: "${id}"
            \t🚫\tkey: "${value}"
            \t🚫\tlist[0]: "${entry}\"""");
  }

  @Test
  void placeholdersWereNotReplaced_withIgnoredPlaceholders() {
    assertThatThrownBy(
            () ->
                Payload.fromJson("{\"id\": \"${id}\", \"key\": \"${value}\"}")
                    .ignorePlaceholders("id", "99911999")
                    .toString())
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌\tFound unused placeholders:
            \t🚫\tkey: "${value}\"""");
  }

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  void placeholdersWereNotReplaced_withIgnoredPlaceholders_all() {
    assertThatNoException()
        .isThrownBy(
            () ->
                Payload.fromJson("{\"id\": \"${id}\", \"key\": \"${value}\"}")
                    .ignorePlaceholders("id", "value")
                    .toString());
  }

  @SuppressWarnings("UnnecessaryToStringCall")
  @Test
  void invalidJson() {
    @Language("json")
    var json = new String(new byte[] {'{', 'a'});
    assertThatThrownBy(() -> log.info("{}", Payload.fromJson(json).toString()))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
❌	Invalid JSON provided. Unexpected character ('a' (code 97)): was expecting double-quote to start field name
 at [Source: REDACTED (`StreamReadFeature.INCLUDE_SOURCE_IN_LOCATION` disabled); line: 1, column: 2]:
{a""");
  }

  @Test
  void extensionMethod() {
    assertThat(Payload.fromJson("{\"custom\": \"${custom}\"}").withCustom("123").toString())
        .isEqualTo(
            Payload.fromJson("{\"custom\": \"${custom}\"}").with("custom", "123").toString());
  }
}
