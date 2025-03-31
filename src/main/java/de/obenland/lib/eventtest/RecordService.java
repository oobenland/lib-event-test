package de.obenland.lib.eventtest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

/**
 * A utility class providing convenience methods to work with Kafka {@code ConsumerRecord}
 * and {@code ProducerRecord} objects. It allows retrieval of headers and conversion
 * between producer and consumer records.
 */
public class RecordService {
  private RecordService() {}

  /**
   * Retrieves the value of the "Content-Type" header from the provided Kafka {@code ConsumerRecord}.
   *
   * @param record the {@code ConsumerRecord} from which to extract the "Content-Type" header
   * @return the value of the "Content-Type" header as a {@code String}, or {@code null} if the header is not present
   */
  public static String contentType(ConsumerRecord<String, String> record) {
    return header(record, "Content-Type");
  }

  /**
   * Retrieves the value of the "Content-ID" header from the provided Kafka {@code ConsumerRecord}.
   *
   * @param record the {@code ConsumerRecord} from which to extract the "Content-ID" header
   * @return the value of the "Content-ID" header as a {@code String}, or {@code null} if the header is not present
   */
  public static String contentId(ConsumerRecord<String, String> record) {
    return header(record, "Content-ID");
  }


  /**
   * Retrieves the value of a specified header from the given Kafka {@code ConsumerRecord}.
   *
   * @param record the {@code ConsumerRecord} from which to extract the header value
   * @param header the name of the header to retrieve
   * @return the value of the header as a {@code String}, or {@code null} if the header is not present or has no value
   */
  public static String header(ConsumerRecord<String, String> record, String header) {
    Header kafkaHeader = record.headers().lastHeader(header);
    return kafkaHeader != null && kafkaHeader.value() != null
        ? new String(kafkaHeader.value(), StandardCharsets.UTF_8)
        : null;
  }

  /**
   * Retrieves the value of the "Content-ID" header from the provided Kafka {@code ProducerRecord}.
   *
   * @param record the {@code ProducerRecord} from which to extract the "Content-ID" header
   * @return the value of the "Content-ID" header as a {@code String}, or {@code null} if the header is not present
   */
  public static String contentId(ProducerRecord<String, String> record) {
    return header(record, "Content-ID");
  }

  /**
   * Retrieves the value of a specified header from the given Kafka {@code ProducerRecord}.
   *
   * @param record the {@code ProducerRecord} from which to extract the header value
   * @param header the name of the header to retrieve
   * @return the value of the header as a {@code String}, or {@code null} if the header is not present or has no value
   */
  public static String header(ProducerRecord<String, String> record, String header) {
    Header kafkaHeader = record.headers().lastHeader(header);
    return kafkaHeader != null && kafkaHeader.value() != null
        ? new String(kafkaHeader.value(), StandardCharsets.UTF_8)
        : null;
  }

  /**
   * Converts a Kafka {@code ProducerRecord} to a {@code ConsumerRecord}.
   *
   * <p>This method transfers the topic, key, value, and headers from the {@code ProducerRecord}
   * to create a new {@code ConsumerRecord}. The partition and offset values are not set (-1).
   *
   * @param record the {@code ProducerRecord} to convert
   * @return a new {@code ConsumerRecord} with data copied from the given {@code ProducerRecord}
   */
  public static ConsumerRecord<String, String> toConsumerRecord(
      ProducerRecord<String, String> record) {
    var consumerRecord = new ConsumerRecord<>(record.topic(), -1, -1, record.key(), record.value());
    record.headers().forEach(header -> consumerRecord.headers().add(header));
    return consumerRecord;
  }
}
