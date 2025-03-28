package de.obenland.lib.eventtest;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.*;
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

@Slf4j
public class Asserter {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final List<Asserter> unfinishedAsserters = new ArrayList<>();
  private final List<Assertion> assertions = new ArrayList<>();
  private final boolean awaitAssertion;
  private Function<Supplier<List<ConsumerRecord<String, String>>>, List<ConsumerRecord<String, String>>> assertionFunction;

  private Asserter(boolean awaitAssertion) {
    this.awaitAssertion = awaitAssertion;
    assertionFunction = awaitAssertion ? this::doAwait : this::doAssert;
    unfinishedAsserters.add(this);
  }

  /**
   * @param codeRepresentation an example how this Assertion is created
   * @param filterFunction filters the incoming list of records. Think of the argument of {@link
   *     java.util.stream.Stream#filter(Predicate) Stream::filter}
   * @param assertionConsumer Checks if the filtered records holds the conditions of this assertion
   * @param successDescriptionFunction Creates a human-readable description of how the filtered
   *     records hold the conditions of this assertion
   * @param failDescriptionFunction Creates a human-readable description of how the incoming list of
   *     records (unfiltered list) didn't hold the conditions of this assertion
   */
  record Assertion(
      String codeRepresentation,
      Predicate<ConsumerRecord<String, String>> filterFunction,
      Consumer<List<ConsumerRecord<String, String>>> assertionConsumer,
      Function<List<ConsumerRecord<String, String>>, String> successDescriptionFunction,
      Function<List<ConsumerRecord<String, String>>, String> failDescriptionFunction) {}

  @CheckReturnValue
  public static Asserter assertEvent() {
    return new Asserter(false).addRecordCountAssertion();
  }

  @CheckReturnValue
  public static Asserter awaitEvent() {
    return new Asserter(true).addRecordCountAssertion();
  }

  public static void sync(SendResult<String, String> sendResult) {
    Asserter.awaitEvent().withSendResult(sendResult).isCommitted();
  }

  public static void validate() {
    if (unfinishedAsserters.isEmpty()) {
      return;
    }

    var buffer = new StringBuilder("Found unfinished assertions:\n");
    for (var assertion : unfinishedAsserters) {
      buffer.append(assertion.toString()).append("\n");
    }
    unfinishedAsserters.clear();
    throw new AssertionError(buffer.toString());
  }

  @CheckReturnValue
  public Asserter withSendResult(SendResult<String, String> sendResult) {
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

  @CheckReturnValue
  public Asserter withTopic(String topic) {
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

  @CheckReturnValue
  public Asserter withKey(String key) {
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

  @CheckReturnValue
  public Asserter withContentType(String contentType) {
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

  @CheckReturnValue
  public Asserter withContentId(String contentId) {
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

  @CheckReturnValue
  public Asserter withMatchingJsonPath(String jsonPathString) {
    var jsonPath = JsonPath.compile(jsonPathString);
    Predicate<String> jsonPathFilterPredicate =
        json -> {
          try {
            return JsonPath.parse(json).read(jsonPath) != null;
          } catch (PathNotFoundException _) {
            return false;
          }
        };
    assertions.add(
        new Assertion(
            "withMatchingJsonPath(\"" + jsonPathString + "\")",
            record -> jsonPathFilterPredicate.test(record.value()),
            records -> assertThat(records).isNotEmpty(),
            records ->
                "☑️\tFound %d records with JsonPath '%s'".formatted(records.size(), jsonPathString),
            records -> {
              var buffer = new StringBuilder();
              var recordsWithJsonPath =
                  records.stream()
                      .filter(record -> jsonPathFilterPredicate.test(record.value()))
                      .toList();
              if (!recordsWithJsonPath.isEmpty()) {
                buffer
                    .append("\n\tFound records with JsonPath '")
                    .append(jsonPathString)
                    .append("':")
                    .append("\n\t🚫\t")
                    .append(
                        recordsWithJsonPath.stream()
                            .map(ConsumerRecord::value)
                            .filter(jsonPathFilterPredicate)
                            .distinct()
                            .sorted()
                            .collect(Collectors.joining("\n\t🚫\t")));
              }
              return "❌\tFound no records with JsonPath '%s'%s"
                  .formatted(jsonPathString, buffer.toString());
            }));
    return this;
  }

  @CheckReturnValue
  public Asserter withPayload(Payload payload) {
    assertions.add(
        new Assertion(
            "withPayload(\"" + payload + "\")",
            record -> {
              try {
                return JSONCompare.compareJSON(
                        payload.toString(), record.value(), payload.getCompareMode())
                    .passed();
              } catch (JSONException e) {
                return false;
              }
            },
            records -> assertThat(records).isNotEmpty(),
            records ->
                "☑️\tFound %d records with payload:\n%s"
                    .formatted(records.size(), payload.toString().trim()),
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
                            payload.toString(), record.value(), payload.getCompareMode());
                    buffer.append("\t").append(result.getMessage().trim().replace("\n", "\n\t"));
                  } catch (JSONException e) {
                    buffer.append("\t").append(e.getMessage().trim().replace("\n", "\n\t"));
                  }
                }
              }

              return "❌\tFound no records with payload:\n\t%s%s"
                  .formatted(
                      prettyPrintJson(payload.toString()).replace("\n", "\n\t"),
                      buffer.toString());
            }));
    return this;
  }

  @CheckReturnValue
  public Asserter withHeader(String header) {
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

  @CheckReturnValue
  public Asserter withHeader(String header, String value) {
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

  @CheckReturnValue
  public Asserter withHeader(String header, byte[] value) {
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

  @CheckReturnValue
  public Asserter times(int times) {
    assertions.add(
        new Assertion(
            "times(" + times + ")",
            _ -> true,
            records -> assertThat(records).hasSize(times),
            records -> "☑️\tFound %d records as expected".formatted(records.size()),
            records -> "❌\tFound %d records but expected %d".formatted(records.size(), times)));
    return this;
  }

  @CheckReturnValue
  public Asserter one() {
    assertions.add(
        new Assertion(
            "one()",
            _ -> true,
            records -> assertThat(records).hasSize(1),
            records -> "☑️\tFound %d records as expected".formatted(records.size()),
            records -> "❌\tFound %d records but expected one".formatted(records.size())));
    return this;
  }

  @CheckReturnValue
  public Asserter none() {
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

  public List<ConsumerRecord<String, String>> isProduced() {
    return assertionFunction.apply(RecordInterceptor::getProducedRecords);
  }

  public List<ConsumerRecord<String, String>> isCommitted() {
    return assertionFunction.apply(RecordInterceptor::getCommittedRecords);
  }

  public List<ConsumerRecord<String, String>> isConsumed() {
    return assertionFunction.apply(RecordInterceptor::getConsumedRecords);
  }

  private List<ConsumerRecord<String, String>> doAssert(Supplier<List<ConsumerRecord<String, String>>> recordSupplier) {
    unfinishedAsserters.remove(this);

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

  private List<ConsumerRecord<String, String>> doAwait(Supplier<List<ConsumerRecord<String, String>>> recordSupplier) {
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

  private Asserter addRecordCountAssertion() {
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
