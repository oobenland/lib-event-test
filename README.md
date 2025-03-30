# libEventTest

Asserting Kafka events in a Spring Boot application with ease.

Working with an distributed event-driven system is difficult. Writing clean and
easy-to-read integration tests is even more challenging.

This library provides tools to await events and verify their content.
When assertions don't hold it provides detailed error messages and provides
additional information about other events, which nearly match the assertion.

# Install

Add dependency to pom.xml:

    <dependency>
      <groupId>de.obenland.lib</groupId>
      <artifactId>event-test</artifactId>
      <version>0.0.1-SNAPSHOT</version>
    </dependency>

Configure a Kafka Interceptor to application.properties:

    spring.kafka.consumer.properties.interceptor.classes=de.obenland.lib.eventtest.RecordInterceptor
    spring.kafka.producer.properties.interceptor.classes=de.obenland.lib.eventtest.RecordInterceptor

The tests have to be configured to clear previous received events, and it's
encouraged to validate the correct usage of assertions after each test. This
checks for assertions which are set up, but never executed, because the final
call to `isProduced`, `isConsumed` or `isCommitted` is missing.

```java

@BeforeEach
void setUp() {
  RecordInterceptor.clear();
}

@AfterEach
void tearDown() {
  EventAsserter.validate();
}
```

You can use static imports to improve the readability of your tests:

```java
import static de.obenland.lib.eventtest.EventAsserter.assertEvent;
import static de.obenland.lib.eventtest.EventAsserter.sync;
import static de.obenland.lib.eventtest.EventPayload.fromFile;
import static de.obenland.lib.eventtest.EventPayload.fromJson;
```

# Example

```java
// Send an event to Kafka, e.g. inside your production code
kafkaTemplate.send(record);

// Wait for an event
awaitEvent().withTopic("test.topic")
  .withKey("test.key")
  .withContentType("test.contentType")
  .isProduced();
```

# Usage

Let's assume our application sends an event to Kafka and we want to verify it.
We start by awaiting events:

```java
awaitEvent()
  [...]
```

This creates a context where we can express our assertions. For example
we want to verify the topic where a event was sent to, the key and content type
of the event. We can chain different requirements, which will be checked in
given order:

```java
awaitEvent()
  .withTopic("test.topic")
  .withKey("test.key")
  .withContentType("test.contentType")
  [...]
```

But this doesn't do anything. We have to tell the assertion where to look for
the event. There are three stages of the rout of an event:

- isProduced \
  The event was sent by the producer. This is useful to check if our application
  creates the correct events.
- isConsumed \
  The event listener of our application processed the event
- isCommitted \
  The event listener commited the offset of a consumed event

Let's say we want to check our application to create the correct events:

```java
awaitEvent()
  .withTopic("test.topic")
  .withKey("test.key")
  .withContentType("test.contentType")
  .isProduced();
```

## API

Checks for the topic

```java
withTopic(String topic)
```

Checks for the key:

```java
withKey(String key)
```

Checks for a header 'Content-Type' with given value

```java
withContentType(String contentType)
```

Checks for a header 'Content-Id' with given value

```java
withContentId(String contentId)
```

Checks for a specific header. You can check for header name or header name with
value

```java
withHeader(...)
```

Checks for the number of events which passes previously provided requirements

```java
times(int times)
```

Requires there is only one event which matches

```java
one()
```

Requires no event matches given requirements

```java
none()
```

Checks for the eventPayload/content of the event.

```java
withPayload(Payload eventPayload)
```

## Payload

You can check for the eventPayload of an event by providing a template and
enrich it by replacing placeholders. 

Placeholders begin with `${`, followed by
an identifier and end with `}`. You can use them anywhere.

```java
awaitEvent()
  .withPayload(fromJson(
      """
      {
        "id": "${id}",
        "value": "${value}"
      }
      """)
      .withId("123")
      .wtih("value","foo bar"))
  .isProduced();
```

There are two predefined placeholders with their own method:

```java
withId(String id); // for ${id}

withTimestamp(String timestamp); // for ${timestamp}

withTimestamp(Instant timestamp); // for ${timestamp}
```

But you can choose any placeholder as you like, and provide it with:

```java
with(String key, String value);
```

And you can add your own specialized methods
using [Lombok's @ExtensionMethod](https://projectlombok.org/features/experimental/ExtensionMethod).

### Arrays in Payloads

Arrays have got special treatment. Insted of providing payload templates with
different sized arrays, you can provide an array with one entry and configure
the array inside your test:

```java
var entries = List.of(
    // some records with id and name fields.
);

awaitEvent()
  .withPayload(fromJson(
      """
      {
        "id": "${id}"
        "entries": [
          { 
            "id": "${id}",
            "name": "${name}"
          }
        ]
      }
      """)
      .withId(123)
      .withArray(
          "/entries",
          entries,
          (payload, entry) -> {
            payload.withId(entry.id())
                   .with("name", entry.name())
          }))
  .isProduced();
```

Configuring arrays create a different context, so there are no naming collisions
between placeholders outside and inside of the array.


