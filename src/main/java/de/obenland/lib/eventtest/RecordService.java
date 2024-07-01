package de.obenland.lib.eventtest;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;

import java.nio.charset.StandardCharsets;

public class RecordService {

  public static String contentType(ConsumerRecord<String, String> record) {
    return header(record, "Content-Type");
  }

  public static String contentId(ConsumerRecord<String, String> record) {
    return header(record, "Content-ID");
  }

  public static String header(ConsumerRecord<String, String> record, String header) {
    Header kafkaHeader = record.headers().lastHeader(header);
    return kafkaHeader != null && kafkaHeader.value() != null
        ? new String(kafkaHeader.value(), StandardCharsets.UTF_8)
        : null;
  }

  public static String contentId(ProducerRecord<String, String> record) {
    return header(record, "Content-ID");
  }

  public static String header(ProducerRecord<String, String> record, String header) {
    Header kafkaHeader = record.headers().lastHeader(header);
    return kafkaHeader != null && kafkaHeader.value() != null
        ? new String(kafkaHeader.value(), StandardCharsets.UTF_8)
        : null;
  }

  public static ConsumerRecord<String, String> toConsumerRecord(
      ProducerRecord<String, String> record) {
    var consumerRecord = new ConsumerRecord<>(record.topic(), -1, -1, record.key(), record.value());
    record.headers().forEach(header -> consumerRecord.headers().add(header));
    return consumerRecord;
  }
}
