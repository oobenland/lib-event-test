package it.obenland.lib.eventtest;

import static it.obenland.lib.eventtest.RecordService.toConsumerRecord;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerInterceptor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * A Kafka interceptor for both producer and consumer records that provides hooks for intercepting
 * and monitoring record processing. This class is designed to capture and store records for
 * analysis and testing purposes.
 *
 * <p>Before each test case the RecordInterceptor has to be cleared:
 *
 * <pre>{@code
 * @BeforeEach
 * void setUp() {
 *   RecordInterceptor.clear();
 * }
 * }</pre>
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class RecordInterceptor
    implements ConsumerInterceptor<String, String>, ProducerInterceptor<String, String> {
  @Getter
  private static final List<ConsumerRecord<String, String>> consumedRecords =
      new CopyOnWriteArrayList<>();

  @Getter
  private static final List<ConsumerRecord<String, String>> committedRecords =
      new CopyOnWriteArrayList<>();

  @Getter
  private static final List<ConsumerRecord<String, String>> producedRecords =
      new CopyOnWriteArrayList<>();

  /**
   * Clears all captured records (consumed, committed, and produced records).
   *
   * <p>This method must be called before each test or recording session to ensure no stale data is
   * present.
   */
  public static void clear() {
    consumedRecords.clear();
    committedRecords.clear();
    producedRecords.clear();
  }

  /**
   * Intercepts and records all consumed Kafka records.
   *
   * @param records The records that were consumed from Kafka.
   * @return The same set of records as passed, for further processing.
   */
  @Override
  public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
    records.forEach(record -> consumedRecords.add(toStringConsumerRecord(record)));
    return records;
  }

  /**
   * Records committed offsets for consumed records, based on the supplied offsets.
   *
   * @param offsets A map of Kafka topic-partitions to their corresponding committed offsets.
   */
  @Override
  public void onCommit(Map<TopicPartition, OffsetAndMetadata> offsets) {
    for (var entry : offsets.entrySet()) {
      consumedRecords.stream()
          .filter(record -> Objects.equals(record.topic(), entry.getKey().topic()))
          .filter(record -> Objects.equals(record.partition(), entry.getKey().partition()))
          .filter(record -> record.offset() == entry.getValue().offset() - 1)
          .findFirst()
          .ifPresent(committedRecords::add);
    }
  }

  /**
   * Intercepts and records producer records after transforming them to consumer records.
   *
   * @param record The record that is being sent to Kafka.
   * @return The original producer record, as is, for further processing.
   */
  @Override
  public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
    producedRecords.add(toConsumerRecord(toStringProducerRecord(record)));
    return record;
  }

  /**
   * Acknowledgement callback for producer records. Currently does nothing.
   *
   * @param metadata Metadata about the acknowledged record.
   * @param exception Exception encountered during acknowledgment, if any.
   */
  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

  /** Closes the interceptor. No specific clean-up actions are needed for this implementation. */
  @Override
  public void close() {}

  /**
   * Configures the interceptor with the provided configuration settings.
   *
   * @param configs A map of configuration settings for this interceptor.
   */
  @Override
  public void configure(Map<String, ?> configs) {}

  private ConsumerRecord<String, String> toStringConsumerRecord(ConsumerRecord<String, String> record) {
    return new ConsumerRecord<>(
        record.topic(),
        record.partition(),
        record.offset(),
        record.timestamp(),
        record.timestampType(),
        record.serializedKeySize(),
        record.serializedValueSize(),
        toString(record.key()),
        toString(record.value()),
        record.headers(),
        record.leaderEpoch());
  }

  private ProducerRecord<String, String> toStringProducerRecord(ProducerRecord<String, String> record) {
    final var string = toString(record.key());
    return new ProducerRecord<>(
        record.topic(),
        record.partition(),
        record.timestamp(),
        string,
        toString(record.value()),
        record.headers());
  }

  /**
   * Converts the input object to its string representation based on its type.
   * <p>
   * When using Kafka Streams this interceptor receives byte[], even though a String is expected.
   *
   * @param input the object to be converted to a string representation. Currently supported: String, byte[], null
   * @return the string representation of the input object if supported; returns null for a null input.
   * @throws IllegalArgumentException if the input object type is not supported.
   */
  private static String toString(Object input) {
    return switch(input) {
      case byte[] bytes -> new String(bytes);
      case String s -> s;
      case null -> null;
      default -> throw new IllegalArgumentException("Unsupported type: " + input.getClass());
    };
  }
}
