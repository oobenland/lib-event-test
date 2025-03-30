package de.obenland.lib.eventtest;

import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.node.ArrayNode;
import lombok.Getter;
import org.intellij.lang.annotations.Language;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.core.io.ClassPathResource;

public class EventPayload {
  private static final ObjectMapper objectMapper = new ObjectMapper();
  private final List<String> ignoredPlaceholders = new ArrayList<>();
  private final Map<String, String> placeholderValues = new HashMap<>();
  private String payload;
  @Getter private JSONCompareMode compareMode = JSONCompareMode.LENIENT;

  public EventPayload(String payload) {
    this.payload = payload;
  }

  public static EventPayload fromJson(@Language("json") String json) {
    return new EventPayload(json);
  }

  public static EventPayload fromFile(String path) {
    var resource = new ClassPathResource(path);
    try (var inputStream = new FileInputStream(resource.getFile())) {
      return fromJson(new String(inputStream.readAllBytes(), Charset.defaultCharset()));
    } catch (IOException e) {
      throw new RuntimeException("Failed to load payload from file '%s'".formatted(path), e);
    }
  }

  public EventPayload withId(String id) {
    return with("id", id);
  }

  public EventPayload withTimestamp(String timestamp) {
    return with("timestamp", timestamp);
  }

  public EventPayload withTimestamp(Instant timestamp) {
    return withTimestamp(timestamp.toString());
  }

  public EventPayload with(String key, Object value) {
    if (!payload.contains(toPlaceholder(key))) {
      throw new AssertionError(
          "\n❌\tCan not find placeholder '%s' in payload:\n%s"
              .formatted(toPlaceholder(key), payload));
    }
    placeholderValues.put(key, value.toString());
    return this;
  }

  /**
   * @param jsonPath path to array node<br>
   *     E.g.: <code>/my/path/to/array</code>, <code>/my/array/2/other/array</code>
   * @param values each value is used to create one entry in the array
   * @param configurator configures the payload of each entry for each value
   */
  public <T> EventPayload withArray(
      String jsonPath, Collection<T> values, BiConsumer<EventPayload, T> configurator) {
    JsonPointer jsonPointer;
    try {
      jsonPointer = JsonPointer.compile(jsonPath);
    } catch (Exception e) {
      throw new AssertionError(
          "\n❌\tGiven jsonPath '%s' is not a valid JsonPointer\n".formatted(jsonPath));
    }

    try {
      var jsonRoot = objectMapper.readTree(payload);
      var node = jsonRoot.at(jsonPointer);
      if (node == null) {
        throw new AssertionError(
            "\n❌\tCan not find json path '%s' in payload:\n%s".formatted(jsonPath, payload));
      }

      if (!node.isArray()) {
        throw new AssertionError(
            "\n❌\tNode at json path '%s' is not an array:\n%s".formatted(jsonPath, payload));
      }

      var arrayNode = (ArrayNode) node;
      var entry = arrayNode.remove(0);
      values.stream()
          .map(value -> {
            final var entryPayload = new EventPayload(entry.toPrettyString());
            configurator.accept(entryPayload, value);
            return entryPayload;
          })
          .map(
              entryPayload -> {
                try {
                  return objectMapper.readTree(entryPayload.toString());
                } catch (Exception e) {
                  throw new AssertionError(
                      "\n❌\tFailed to parse payload:\n%s".formatted(payload), e);
                }
              })
          .forEach(arrayNode::add);
      payload = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(jsonRoot);
      return this;
    } catch (Exception e) {
      throw new AssertionError("\n❌\tFailed to parse payload:\n%s".formatted(payload), e);
    }
  }

  public EventPayload lenient() {
    compareMode = JSONCompareMode.LENIENT;
    return this;
  }

  public EventPayload strict() {
    compareMode = JSONCompareMode.STRICT;
    return this;
  }

  public EventPayload strictOrder() {
    compareMode = JSONCompareMode.STRICT_ORDER;
    return this;
  }

  public EventPayload nonExtensible() {
    compareMode = JSONCompareMode.NON_EXTENSIBLE;
    return this;
  }

  public EventPayload ignorePlaceholder(String ignoredPlaceholder) {
    this.ignoredPlaceholders.add(toPlaceholder(ignoredPlaceholder));
    return this;
  }

  public EventPayload ignorePlaceholders(String... placeholders) {
    for (String placeholder : placeholders) {
      ignorePlaceholder(placeholder);
    }
    return this;
  }

  private static String toPlaceholder(String placeholder) {
    return "${" + placeholder + "}";
  }

  private static String toPlaceholderRegex(String placeholder) {
    return "\\$\\{" + placeholder + "}";
  }

  private static String fromPlaceholder(String placeholder) {
    return placeholder.substring(2, placeholder.length() - 1);
  }

  public String toString() {
    placeholderValues.forEach(
        (key, value) -> payload = payload.replaceAll(toPlaceholderRegex(key), value));
    var keyValueMap = jsonToKeyValueMap(payload);
    var unusedPlaceholders =
        keyValueMap.entrySet().stream()
            .filter(entry -> containsPlaceholder(entry.getValue()))
            .filter(entry -> !containsIgnoredPlaceholder(entry.getValue()))
            .toList();
    if (!unusedPlaceholders.isEmpty()) {
      var unusedPlaceholdersList = unusedPlaceholders.stream()
          .map(entry -> entry.getKey() + ": " + entry.getValue())
          .sorted()
          .collect(Collectors.joining("\n\t🚫\t"));

      var ignoredPlaceholdersList = ignoredPlaceholders.stream()
          .map(EventPayload::fromPlaceholder)
          .sorted()
          .collect(Collectors.joining("\n\t→\t"));
      var ignoredPlaceholdersMessage = ignoredPlaceholdersList.isEmpty()
          ? "☑️\tNo placeholders are ignored"
          : "☑️\tFound ignored placeholders:\n\t→\t%s".formatted(ignoredPlaceholdersList);

      var usedPlaceholdersList = placeholderValues.keySet().stream()
          .sorted()
          .collect(Collectors.joining("\n\t→\t"));
      var providedPlaceholdersMessage = usedPlaceholdersList.isEmpty()
          ? "☑️\tNo placeholders are provided"
          : "☑️\tFound provided placeholders:\n\t→\t%s".formatted(usedPlaceholdersList);

      throw new AssertionError("\n%s\n%s\n❌\tFound unused placeholders:\n\t🚫\t%s\nin payload:\n%s".formatted(providedPlaceholdersMessage, ignoredPlaceholdersMessage, unusedPlaceholdersList, payload));
    }

    return payload;
  }

  private static boolean containsPlaceholder(String text) {
    var startIndex = text.indexOf("${");
    if (startIndex == -1) {
      return false;
    }
    var endIndex = text.indexOf("}", startIndex + 2);
    return endIndex != -1;
  }

  private boolean containsIgnoredPlaceholder(String text) {
    return ignoredPlaceholders.stream().anyMatch(text::contains);
  }

  private Map<String, String> jsonToKeyValueMap(String json) {
    try {
      JsonNode node = objectMapper.readTree(json);
      return process("", node);
    } catch (JsonProcessingException e) {
      throw new AssertionError("\n❌\tInvalid JSON provided. " + e.getMessage() + ":\n" + json);
    }
  }

  private static Map<String, String> process(String prefix, JsonNode currentNode) {
    Map<String, String> result = new HashMap<>();
    if (currentNode.isArray()) {
      var node = currentNode.elements();
      var index = 0;
      while (node.hasNext()) {
        result.putAll(process(prefix + "[" + index++ + "]", node.next()));
      }
    } else if (currentNode.isObject()) {
      currentNode
          .fields()
          .forEachRemaining(
              entry ->
                  result.putAll(
                      process(
                          (!prefix.isEmpty() ? prefix + "." : "") + entry.getKey(),
                          entry.getValue())));
    } else {
      result.put(prefix, currentNode.toString());
    }
    return result;
  }
}
