package de.obenland.lib.eventtest;

import static de.obenland.lib.eventtest.RecordService.toConsumerRecord;

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

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

  public void clear() {
    consumedRecords.clear();
    committedRecords.clear();
    producedRecords.clear();
  }

  @Override
  public ConsumerRecords<String, String> onConsume(ConsumerRecords<String, String> records) {
    records.forEach(consumedRecords::add);
    return records;
  }

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

  @Override
  public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
    producedRecords.add(toConsumerRecord(record));
    return record;
  }

  @Override
  public void onAcknowledgement(RecordMetadata metadata, Exception exception) {}

  @Override
  public void close() {}

  @Override
  public void configure(Map<String, ?> configs) {}
}
