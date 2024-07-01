package de.obenland.lib;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.TopicBuilder;

@SpringBootApplication
@Slf4j
@EnableKafka
public class TestApplication {
  @KafkaListener(topicPattern = "^.*$", id = "eventtest.all-topics")
  void consume(ConsumerRecord<String, String> record) {}

  @Bean
  public NewTopic testTopic() {
    return TopicBuilder.name("test.topic").partitions(1).replicas(1).build();
  }

  @Bean
  public NewTopic unsynchedTestTopic() {
    return TopicBuilder.name("test.topic.unsynched").partitions(1).replicas(1).build();
  }
}
