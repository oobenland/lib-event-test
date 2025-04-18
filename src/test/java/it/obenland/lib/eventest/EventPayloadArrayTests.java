package it.obenland.lib.eventest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import it.obenland.lib.TestPayloadExtensions;
import it.obenland.lib.eventtest.EventPayload;
import java.util.List;
import lombok.experimental.ExtensionMethod;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;

@Slf4j
@ExtensionMethod(TestPayloadExtensions.class)
public class EventPayloadArrayTests {

  @Test
  void happyPath() {
    assertThat(
            EventPayload.fromJson(
                    """
                    {
                      "id": "${id}",
                      "myArray": [
                        {
                          "id": "${id}"
                        }
                      ]
                    }
                    """)
                .withId("0")
                .withArray("/myArray", List.of("1", "2"), EventPayload::withId)
                .toString())
        .hasToString(
            """
            {
              "id" : "0",
              "myArray" : [ {
                "id" : "1"
              }, {
                "id" : "2"
              } ]
            }\
            """);
  }

  @Test
  void longPath() {
    assertThat(
            EventPayload.fromJson(
                    """
                    {
                      "id": "${id}",
                      "my": {
                          "object": {
                            "myArray": [
                            {
                              "id": "${id}",
                              "value": "${value}"
                            }
                          ]
                        }
                      }
                    }
                    """)
                .withId("0")
                .withArray(
                    "/my/object/myArray",
                    List.of("1", "2"),
                    (payload, id) -> {
                      payload.withId(id).with("value", "myValue");
                    })
                .toString())
        .hasToString(
            """
            {
              "id" : "0",
              "my" : {
                "object" : {
                  "myArray" : [ {
                    "id" : "1",
                    "value" : "myValue"
                  }, {
                    "id" : "2",
                    "value" : "myValue"
                  } ]
                }
              }
            }\
            """);
  }

  @Test
  void emptyArray() {
    assertThat(
            EventPayload.fromJson(
                    """
                    {
                      "id": "${id}",
                      "myArray": [
                        {
                          "id": "${id}"
                        }
                      ]
                    }
                    """)
                .withId("0")
                .withArray("/myArray", List.of(), EventPayload::withId)
                .toString())
        .hasToString(
            """
            {
              "id" : "0",
              "myArray" : [ ]
            }\
            """);
  }

  @Test
  void longPathInsideArray() {
    assertThat(
            EventPayload.fromJson(
                    """
                    {
                      "id": "${id}",
                      "my": {
                        "objects": [
                          {
                            "myArray": [
                              {
                                "id": "${id}",
                                "value": "${value}"
                              }
                            ]
                          }
                        ]
                      }
                    }
                    """)
                .withId("0")
                .withArray(
                    "/my/objects/0/myArray",
                    List.of("1", "2"),
                    (payload, id) -> {
                      payload.withId(id).with("value", "myValue");
                    })
                .toString())
        .hasToString(
            """
            {
              "id" : "0",
              "my" : {
                "objects" : [ {
                  "myArray" : [ {
                    "id" : "1",
                    "value" : "myValue"
                  }, {
                    "id" : "2",
                    "value" : "myValue"
                  } ]
                } ]
              }
            }\
            """);
  }

  @Test
  void nestedArray() {
    assertThat(
            EventPayload.fromJson(
                    """
                    {
                      "id": "${id}",
                      "firstArray": [
                        {
                          "firstId": "${id}",
                          "secondArray": [
                            {
                              "secondId": "${id}",
                              "value": "${value}"
                            }
                          ]
                        }
                      ]
                    }
                    """)
                .withId("0")
                .withArray(
                    "/firstArray",
                    List.of("1", "2"),
                    (payload, id) -> {
                      payload
                          .withId(id)
                          .withArray(
                              "/secondArray",
                              List.of("3", "4"),
                              (innerPayload, innerId) -> {
                                innerPayload.withId(id + " - " + innerId);
                                innerPayload.with("value", "myValue");
                              });
                    })
                .toString())
        .hasToString(
            """
            {
              "id" : "0",
              "firstArray" : [ {
                "firstId" : "1",
                "secondArray" : [ {
                  "secondId" : "1 - 3",
                  "value" : "myValue"
                }, {
                  "secondId" : "1 - 4",
                  "value" : "myValue"
                } ]
              }, {
                "firstId" : "2",
                "secondArray" : [ {
                  "secondId" : "2 - 3",
                  "value" : "myValue"
                }, {
                  "secondId" : "2 - 4",
                  "value" : "myValue"
                } ]
              } ]
            }\
            """);
  }

  @Test
  void isNotAnArray() {
    assertThatThrownBy(
            () ->
                EventPayload.fromJson(
                        """
                        {
                          "notAnArray": "${value}"
                        }
                        """)
                    .withArray("/notAnArray", List.of("1", "2"), EventPayload::withId))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌	Node at json path '/notAnArray' is not an array:
            {
              "notAnArray": "${value}"
            }
            """);
  }

  @Test
  void isNotAnJsonPath() {
    assertThatThrownBy(() -> EventPayload.fromJson("{}").withArray("invalid.path", null, null))
        .isInstanceOf(AssertionError.class)
        .hasMessageContaining(
            """
            ❌	Given jsonPath 'invalid.path' is not a valid JsonPointer
            """);
  }

  @Test
  void extensionMethod() {
    assertThat(
            EventPayload.fromJson("{\"array\": [{\"id\": \"${id}\"}]}")
                .withCustomArray("/array", List.of("1", "2"))
                .toString())
        .hasToString(
            """
            {
              "array" : [ {
                "id" : "1"
              }, {
                "id" : "2"
              } ]
            }\
            """);
  }
}
