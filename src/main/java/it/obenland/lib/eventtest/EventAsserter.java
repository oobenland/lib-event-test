package it.obenland.lib.eventtest;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.awaitility.Awaitility;
import org.jetbrains.annotations.CheckReturnValue;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONCompare;
import org.springframework.kafka.support.SendResult;

/**
 * The EventAsserter class provides functionality to create and validate assertions of Kafka events.
 * It allows filtering, validating, and verifying event content based on specific criteria such as
 * topic, key or headers.
 *
 * <p>This class maintains a record of unfinished assertions to ensure all validation checks are
 * completed. It provides flexibility in building assertions with or without awaiting mechanisms.
 *
 * <p>It's encouraged to validate the correct usage of assertions after each test. This checks for
 * assertions which are set up, but never executed, because the final call to <code>isProduced()
 * </code>, <code>isConsumed()</code> or <code>isCommitted()</code> is missing.
 *
 * <pre>{@code
 * @AfterEach
 * void tearDown() {
 *   EventAsserter.validate();
 * }
 * }</pre>
 */
@Slf4j
public class EventAsserter {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final List<EventAsserter> UNFINISHED_EVENT_ASSERTERS = new ArrayList<>();
  private final List<Assertion> assertions = new ArrayList<>();
  private final boolean awaitAssertion;
  private Function<
          Supplier<List<ConsumerRecord<String, String>>>, List<ConsumerRecord<String, String>>>
      assertionFunction;

  private EventAsserter(boolean awaitAssertion) {
    this.awaitAssertion = awaitAssertion;
    assertionFunction = awaitAssertion ? this::doAwait : this::doAssert;
    UNFINISHED_EVENT_ASSERTERS.add(this);
  }

  /**
   * @param codeRepresentation a string representation of the method call which created this
   *     assertion. E.g. <code>withTopic(String)</code> has the value <code>
   *                                   "withTopic(\"" + topic + "\")"</code>, which results in a
   *     code representation of <code>
   *                                   withTopic("test.topic")</code> when called with <code>
   *     "test.topic"</code> as parameter
   * @param filterFunction filters the incoming list of records. Think of the argument of {@link
   *     java.util.stream.Stream#filter(Predicate) Stream::filter}
   * @param assertionConsumer Checks if the filtered records holds the conditions of this assertion
   * @param successDescriptionFunction Creates a human-readable description of how the filtered
   *     records hold the conditions of this assertion
   * @param failDescriptionFunction Creates a human-readable description of how the unfiltered
   *     incoming list of records didn't hold the conditions of this assertion
   */
  record Assertion(
      String codeRepresentation,
      Predicate<ConsumerRecord<String, String>> filterFunction,
      Consumer<List<ConsumerRecord<String, String>>> assertionConsumer,
      Function<List<ConsumerRecord<String, String>>, String> successDescriptionFunction,
      Function<List<ConsumerRecord<String, String>>, String> failDescriptionFunction) {}

  /**
   * Creates and returns an {@code EventAsserter} instance to validate Kafka events synchronously.
   *
   * <p>This method does not wait for events and immediately checks for events based on assertions.
   *
   * @return a new {@code EventAsserter} instance for event validation
   */
  @CheckReturnValue
  public static EventAsserter assertEvent() {
    return new EventAsserter(false).addRecordCountAssertion();
  }

  /**
   * Creates and returns an {@code EventAsserter} instance to validate Kafka events asynchronously.
   *
   * <p>This method waits for events to be available before validating assertions.
   *
   * @return a new {@code EventAsserter} instance for asynchronous event validation
   */
  @CheckReturnValue
  public static EventAsserter awaitEvent() {
    return new EventAsserter(true).addRecordCountAssertion();
  }

  /**
   * Validates and ensures a Kafka {@code SendResult} is properly committed.
   *
   * <p>Allows sending a Kafka event using {@link org.springframework.kafka.core.KafkaTemplate#send
   * KafkaTemplate::send} and waiting for this event to be processed.
   *
   * @param sendResult the {@code SendResult} object containing Kafka metadata to validate
   */
  public static void sync(SendResult<String, String> sendResult) {
    EventAsserter.awaitEvent().withSendResult(sendResult).isCommitted();
  }

  /**
   * Validates that all assertions created during tests have been executed.
   *
   * <p>If any assertions are unfinished, an {@code AssertionError} is thrown with a detailed
   * message about the pending assertions.
   *
   * @throws AssertionError if there are unfinished assertions
   */
  public static void validate() {
    if (UNFINISHED_EVENT_ASSERTERS.isEmpty()) {
      return;
    }

    var buffer = new StringBuilder("Found unfinished assertions:\n");
    for (var assertion : UNFINISHED_EVENT_ASSERTERS) {
      buffer.append(assertion.toString()).append("\n");
    }
    UNFINISHED_EVENT_ASSERTERS.clear();
    throw new AssertionError(buffer.toString());
  }

  /**
   * Filters records to match the specified Kafka {@code SendResult}'s metadata.
   *
   * @param sendResult the Kafka {@code SendResult} to match records against
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter withSendResult(SendResult<String, String> sendResult) {
    var topic = sendResult.getRecordMetadata().topic();
    var partition = sendResult.getRecordMetadata().partition();
    var offset = sendResult.getRecordMetadata().offset();

    var contentId = RecordService.contentId(sendResult.getProducerRecord());
    assertions.add(
        new Assertion(
            "withSendResult(\"" + sendResult + "\")",
            record -> Objects.equals(RecordService.contentId(record), contentId),
            records -> assertThat(records).isNotEmpty(),
            records ->
                "☑️\tFound %d records with topic '%s', partition %d and offest %d"
                    .formatted(records.size(), topic, partition, offset),
            records -> {
              var buffer = new StringBuilder();
              if (records != null && !records.isEmpty()) {
                buffer.append("\n\tFound records:");
                var sortedRecords =
                    records.stream().sorted(Comparator.comparing(ConsumerRecord::topic)).toList();
                for (var record : sortedRecords) {
                  buffer
                      .append("\n\t🚫\ttopic: '")
                      .append(record.topic())
                      .append("', content id: '")
                      .append(RecordService.contentId(record))
                      .append("'");
                }
              }
              return "❌\tFound no records with topic '%s', partition %d and offest %d%s"
                  .formatted(topic, partition, offset, buffer.toString());
            }));
    return this;
  }


  /**
   * Adds an assertion to filter Kafka records by the specified topic.
   *
   * <p>This assertion ensures that the Kafka event records being processed contain the specified
   * topic. If no records matching the topic are found, the assertion fails.
   *
   * @param topic the topic to match in the event records
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter withTopic(String topic) {
    assertions.add(
        new Assertion(
            "withTopic(\"" + topic + "\")",
            record -> Objects.equals(record.topic(), topic),
            records -> assertThat(records).isNotEmpty(),
            records -> "☑️\tFound %d records with topic '%s'".formatted(records.size(), topic),
            records -> {
              var buffer = new StringBuilder();
              if (records != null && !records.isEmpty()) {
                buffer
                    .append("\n\tFound records with topics:")
                    .append("\n\t🚫\t")
                    .append(
                        records.stream()
                            .map(ConsumerRecord::topic)
                            .distinct()
                            .sorted()
                            .collect(Collectors.joining("\n\t🚫\t")));
              }
              return "❌\tFound no records with topic '%s'%s".formatted(topic, buffer.toString());
            }));
    return this;
  }

  
  /**
   * Adds an assertion to filter Kafka records by the specified key.
   *
   * <p>This assertion ensures that the Kafka event records being processed contain the specified
   * key. If no matching records are found, the assertion fails.
   *
   * @param key the key to match in the event records
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter withKey(String key) {
    assertions.add(
        new Assertion(
            "withKey(\"" + key + "\")",
            record -> Objects.equals(record.key(), key),
            records -> assertThat(records).isNotEmpty(),
            records -> "☑️\tFound %d records with key '%s'".formatted(records.size(), key),
            records -> {
              var buffer = new StringBuilder();
              if (records != null && !records.isEmpty()) {
                buffer
                    .append("\n\tFound records with keys:")
                    .append("\n\t🚫\t")
                    .append(
                        records.stream()
                            .map(ConsumerRecord::key)
                            .distinct()
                            .sorted()
                            .collect(Collectors.joining("\n\t🚫\t")));
              }
              return "❌\tFound no records with key '%s'%s".formatted(key, buffer.toString());
            }));
    return this;
  }

  /**
   * Adds an assertion to filter Kafka records by the specified content type.
   *
   * <p>This assertion ensures that the Kafka event records being processed contain the specified
   * content type. If no matching records are found, the assertion fails.
   *
   * @param contentType the content type to match in the event records
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter withContentType(String contentType) {
    assertions.add(
        new Assertion(
            "withContentType(\"" + contentType + "\")",
            record -> Objects.equals(RecordService.contentType(record), contentType),
            records -> assertThat(records).isNotEmpty(),
            records ->
                "☑️\tFound %d records with content type '%s'"
                    .formatted(records.size(), contentType),
            records -> {
              var buffer = new StringBuilder();
              if (records != null && !records.isEmpty()) {
                buffer
                    .append("\n\tFound records with content types:")
                    .append("\n\t🚫\t")
                    .append(
                        records.stream()
                            .map(RecordService::contentType)
                            .distinct()
                            .sorted()
                            .collect(Collectors.joining("\n\t🚫\t")));
              }
              return "❌\tFound no records with content type '%s'%s"
                  .formatted(contentType, buffer.toString());
            }));
    return this;
  }

  /**
   * Adds an assertion to filter Kafka records by the specified content ID.
   *
   * <p>This assertion ensures that the Kafka event records being processed contain the specified
   * content ID. If no matching records are found, the assertion fails.
   *
   * @param contentId the content ID to match in the event records
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter withContentId(String contentId) {
    assertions.add(
        new Assertion(
            "withContentId(\"" + contentId + "\")",
            record -> Objects.equals(RecordService.contentId(record), contentId),
            records -> assertThat(records).isNotEmpty(),
            records ->
                "☑️\tFound %d records with content id '%s'".formatted(records.size(), contentId),
            records -> {
              var buffer = new StringBuilder();
              if (records != null && !records.isEmpty()) {
                buffer
                    .append("\n\tFound records with content ids:")
                    .append("\n\t🚫\t")
                    .append(
                        records.stream()
                            .map(RecordService::contentId)
                            .distinct()
                            .sorted()
                            .collect(Collectors.joining("\n\t🚫\t")));
              }
              return "❌\tFound no records with content id '%s'%s"
                  .formatted(contentId, buffer.toString());
            }));
    return this;
  }

  /**
   * Adds an assertion to filter Kafka records by the specified event payload.
   *
   * <p>This assertion ensures that the Kafka event records being processed contain the specified
   * payload, compared using JSON structure and content. If no matching records are found,
   * the assertion fails.
   *
   * @param eventPayload the payload to match in the event records
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter withPayload(EventPayload eventPayload) {
    assertions.add(
        new Assertion(
            "withPayload(\"" + eventPayload + "\")",
            record -> {
              try {
                return JSONCompare.compareJSON(
                        eventPayload.toString(), record.value(), eventPayload.getCompareMode())
                    .passed();
              } catch (JSONException e) {
                return false;
              }
            },
            records -> assertThat(records).isNotEmpty(),
            records ->
                "☑️\tFound %d records with eventPayload:\n%s"
                    .formatted(records.size(), eventPayload.toString().trim()),
            records -> {
              var buffer = new StringBuilder();
              if (records != null && !records.isEmpty()) {
                buffer.append("\n\tFound records with payloads:");
                for (var record : records) {
                  buffer
                      .append("\n\t🚫\n\t")
                      .append(prettyPrintJson(record.value()).replace("\n", "\n\t"))
                      .append("\n");
                  try {
                    var result =
                        JSONCompare.compareJSON(
                            eventPayload.toString(), record.value(), eventPayload.getCompareMode());
                    buffer.append("\t").append(result.getMessage().trim().replace("\n", "\n\t"));
                  } catch (JSONException e) {
                    buffer.append("\t").append(e.getMessage().trim().replace("\n", "\n\t"));
                  }
                }
              }

              return "❌\tFound no records with eventPayload:\n\t%s%s"
                  .formatted(
                      prettyPrintJson(eventPayload.toString()).replace("\n", "\n\t"),
                      buffer.toString());
            }));
    return this;
  }

  /**
   * Adds an assertion to filter Kafka records by the presence of the specified header.
   *
   * <p>This assertion ensures that the Kafka event records being processed include a header with
   * the specified name. If no matching records are found, the assertion fails.
   *
   * @param header the header name to match in the event records
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter withHeader(String header) {
    assertions.add(
        new Assertion(
            "withHeader(\"" + header + "\")",
            record -> record.headers().lastHeader(header) != null,
            records -> assertThat(records).isNotEmpty(),
            records -> "☑️\tFound %d records with '%s'-header".formatted(records.size(), header),
            records -> {
              var buffer = new StringBuilder();
              if (records != null && !records.isEmpty()) {
                buffer.append("\n\tFound records with headers:");
                for (var record : records) {
                  var keys =
                      Arrays.stream(record.headers().toArray())
                          .map(Header::key)
                          .distinct()
                          .sorted()
                          .collect(Collectors.joining("\n\t\t"));
                  buffer.append("\n\t🚫\t").append(keys);
                }
              }
              return "❌\tFound no records with '%s'-header%s".formatted(header, buffer.toString());
            }));
    return this;
  }

  /**
   * Adds an assertion to filter Kafka records by the specified header and its string value.
   *
   * <p>This assertion ensures that the Kafka event records being processed include a header with
   * the specified name and value. If no matching records are found, the assertion fails.
   *
   * @param header the header name to match in the event records
   * @param value  the expected value of the header
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter withHeader(String header, String value) {
    final var valueAsBytes = value.getBytes();
    assertions.add(
        new Assertion(
            "withHeader(\"" + header + "," + value + "\")",
            record ->
                StreamSupport.stream(record.headers().headers(header).spliterator(), false)
                    .anyMatch(h -> Arrays.equals(h.value(), valueAsBytes)),
            records -> assertThat(records).isNotEmpty(),
            records ->
                "☑️\tFound %d records with '%s'-header with value '%s'"
                    .formatted(records.size(), header, value),
            records -> {
              var buffer = new StringBuilder();
              if (records != null && !records.isEmpty()) {
                buffer.append("\n\tFound records with '").append(header).append("'-headers:");
                for (var record : records) {
                  var values =
                      StreamSupport.stream(record.headers().headers(header).spliterator(), false)
                          .map(Header::value)
                          .map(bytes -> new String(bytes, Charset.defaultCharset()))
                          .distinct()
                          .sorted()
                          .collect(Collectors.joining("\n\t\t"));
                  buffer.append("\n\t🚫\t").append(values);
                }
              }
              return "❌\tFound no records with '%s'-header with value '%s'%s"
                  .formatted(header, value, buffer.toString());
            }));
    return this;
  }

  /**
   * Adds an assertion to filter Kafka records by the specified header and its byte array value.
   *
   * <p>This assertion ensures that the Kafka event records being processed include a header with
   * the specified name and value as a byte array. If no matching records are found, the assertion fails.
   *
   * @param header the header name to match in the event records
   * @param value  the expected byte array value of the header
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter withHeader(String header, byte[] value) {
    assertions.add(
        new Assertion(
            "withHeader(\"" + header + "\", \"" + Arrays.toString(value) + "\")",
            record ->
                StreamSupport.stream(record.headers().headers(header).spliterator(), false)
                    .anyMatch(h -> Arrays.equals(h.value(), value)),
            records -> assertThat(records).isNotEmpty(),
            records ->
                "☑️\tFound %d records with '%s'-header with value '%s'"
                    .formatted(records.size(), header, Arrays.toString(value)),
            records -> {
              var buffer = new StringBuilder();
              if (records != null && !records.isEmpty()) {
                buffer.append("\n\tFound records with '").append(header).append("'-headers:");
                for (var record : records) {
                  var values =
                      StreamSupport.stream(record.headers().headers(header).spliterator(), false)
                          .map(Header::value)
                          .map(Arrays::toString)
                          .distinct()
                          .sorted()
                          .collect(Collectors.joining("\n\t\t"));
                  buffer.append("\n\t🚫\t").append(values);
                }
              }
              return "❌\tFound no records with '%s'-header with value '%s'%s"
                  .formatted(header, Arrays.toString(value), buffer.toString());
            }));
    return this;
  }

  /**
   * Adds an assertion to ensure that the specified number of Kafka records are present.
   *
   * @param times the exact number of records expected
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter times(int times) {
    assertions.add(
        new Assertion(
            "times(" + times + ")",
            _ -> true,
            records -> assertThat(records).hasSize(times),
            records -> "☑️\tFound %d records as expected".formatted(records.size()),
            records -> "❌\tFound %d records but expected %d".formatted(records.size(), times)));
    return this;
  }

  /**
   * Asserts that exactly one Kafka event record is present.
   *
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter one() {
    assertions.add(
        new Assertion(
            "one()",
            _ -> true,
            records -> assertThat(records).hasSize(1),
            records -> "☑️\tFound %d records as expected".formatted(records.size()),
            records -> "❌\tFound %d records but expected one".formatted(records.size())));
    return this;
  }

  /**
   * Adds an assertion to ensure that no Kafka records are present.
   *
   * <p>This assertion waits for a short duration to verify the absence of records, and fails
   * if any records are found during that period.
   *
   * @return this {@code EventAsserter} instance for method chaining
   */
  @CheckReturnValue
  public EventAsserter none() {
    var oldAssertionFunction = assertionFunction;
    assertionFunction =
        recordSupplier -> {
          sleep(200);
          return oldAssertionFunction.apply(recordSupplier);
        };
    assertions.add(
        new Assertion(
            "none()",
            _ -> true,
            records -> assertThat(records).isEmpty(),
            records -> "☑️\tFound %d records as expected".formatted(records.size()),
            records -> "❌\tFound %d records but expected none".formatted(records.size())));
    return this;
  }

  /**
   * Executes all assertions and retrieves the list of produced Kafka records that match.
   *
   * @return the list of produced Kafka records matching the assertions
   */
  public List<ConsumerRecord<String, String>> isProduced() {
    return assertionFunction.apply(RecordInterceptor::getProducedRecords);
  }

  /**
   * Executes all assertions and retrieves the list of committed Kafka records that match.
   *
   * @return the list of committed Kafka records matching the assertions
   */
  public List<ConsumerRecord<String, String>> isCommitted() {
    return assertionFunction.apply(RecordInterceptor::getCommittedRecords);
  }

  /**
   * Executes all assertions and retrieves the list of consumed Kafka records that match.
   *
   * @return the list of consumed Kafka records matching the assertions
   */
  public List<ConsumerRecord<String, String>> isConsumed() {
    return assertionFunction.apply(RecordInterceptor::getConsumedRecords);
  }

  
  private List<ConsumerRecord<String, String>> doAssert(
      Supplier<List<ConsumerRecord<String, String>>> recordSupplier) {
    UNFINISHED_EVENT_ASSERTERS.remove(this);

    var records = recordSupplier.get();
    var filteredRecords = new ArrayList<>(records);
    for (Assertion assertion : assertions) {
      filteredRecords.removeIf(Predicate.not(assertion.filterFunction));
    }

    try {
      assertions.getLast().assertionConsumer.accept(filteredRecords);
      return filteredRecords;
    } catch (AssertionError e) {
      throw new AssertionError(toAssertionErrorDescription(records));
    }
  }

  private List<ConsumerRecord<String, String>> doAwait(
      Supplier<List<ConsumerRecord<String, String>>> recordSupplier) {
    var reference = new AtomicReference<List<ConsumerRecord<String, String>>>();
    Awaitility.await().untilAsserted(() -> reference.set(doAssert(recordSupplier)));
    return reference.get();
  }

  private String toAssertionErrorDescription(List<ConsumerRecord<String, String>> records) {
    var buffer = new StringBuilder();
    for (var assertion : assertions) {
      var filteredRecords = records.stream().filter(assertion.filterFunction).toList();
      try {
        assertion.assertionConsumer.accept(filteredRecords);
        buffer.append('\n').append(assertion.successDescriptionFunction.apply(filteredRecords));
      } catch (AssertionError _) {
        buffer.append('\n').append(assertion.failDescriptionFunction.apply(records));
      }
      records = filteredRecords;
    }
    return buffer.toString();
  }

  private EventAsserter addRecordCountAssertion() {
    assertions.addFirst(
        new Assertion(
            null,
            _ -> true,
            records -> assertThat(records).isNotEmpty(),
            records -> "☑️\tFound %d records".formatted(records.size()),
            _ -> "❌\tFound no records"));
    return this;
  }

  private static String prettyPrintJson(String payload) {
    try {
      var jsonObject = OBJECT_MAPPER.readValue(payload, Object.class);
      return OBJECT_MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(jsonObject);
    } catch (JsonProcessingException e) {
      throw new AssertionError(e);
    }
  }

  @SneakyThrows
  private static void sleep(int millis) {
    Thread.sleep(millis);
  }

  @Override
  public String toString() {
    var buffer = new StringBuilder("❌\t");
    if (awaitAssertion) {
      buffer.append("awaitEvent()\n");
    } else {
      buffer.append("assertEvent()\n");
    }
    for (var assertion : assertions) {
      var codeRepresentation = assertion.codeRepresentation();
      if (codeRepresentation == null) {
        continue;
      }
      buffer.append("\t\t.").append(codeRepresentation.replace("\n", "\t\t\n")).append("\n");
    }
    return buffer.toString().trim();
  }
}
