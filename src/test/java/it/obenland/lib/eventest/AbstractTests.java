package it.obenland.lib.eventest;

import it.obenland.lib.eventtest.EventAsserter;
import it.obenland.lib.eventtest.RecordInterceptor;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.intellij.lang.annotations.Language;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
abstract class AbstractTests {
  static final String KAFKA_IMAGE = "confluentinc/cp-kafka:7.8.0";

  @Autowired KafkaTemplate<String, String> kafkaTemplate;

  @BeforeEach
  void setUp() {
    RecordInterceptor.clear();
  }

  @AfterEach
  void tearDown() {
    EventAsserter.validate();
  }

  SendResult<String, String> sendTestEvent() {
    return sendTestEvent(
        "test.topic",
        """
        {
          "id": 1,
          "value": "12345678"
        }
        """);
  }

  void assertIsTestEvent(ConsumerRecord<String, String> revent) {
    assertThat(revent.topic()).isEqualTo("test.topic");
    assertThat(revent.value()).isEqualTo("""
        {
          "id": 1,
          "value": "12345678"
        }
        """);
  }

  SendResult<String, String> sendTestEvent(String topic, @Language("json") String payload) {
    var record = new ProducerRecord<>(topic, "test.key", payload);
    record.headers().add(new RecordHeader("Content-Type", "test.contentType".getBytes()));
    record.headers().add(new RecordHeader("Content-ID", "test.contentId".getBytes()));

    record.headers().add(new RecordHeader("test", "test.header".getBytes()));
    record.headers().add(new RecordHeader("test", "test.header.2".getBytes()));
    try {
      return kafkaTemplate.send(record).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    } finally {
      kafkaTemplate.flush();
    }
  }
}
