# lib-event-test

Test framework to assert events using Kafka and Spring-Boot

# Install

Build and install:

    ./mvnw install

Add to pom.xml:

    <dependency>
      <groupId>de.obenland.lib</groupId>
      <artifactId>event-test</artifactId>
      <version>0.0.1-SNAPSHOT</version>
    </dependency>

Add to application.properties:

    spring.kafka.consumer.properties.interceptor.classes=de.obenland.lib.eventtest.RecordInterceptor
    spring.kafka.producer.properties.interceptor.classes=de.obenland.lib.eventtest.RecordInterceptor

# Usage

    assertEvent()
      .withTopic("test.topic")
      .withKey("test.key")
      .withContentType("test.contentType")
      .isProduced();