package de.obenland.lib.eventtest;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;
import lombok.Getter;
import org.intellij.lang.annotations.Language;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.springframework.core.io.ClassPathResource;

public class Payload {
  private String payload;
  private List<String> ignoredPlaceholders;
  @Getter private JSONCompareMode compareMode = JSONCompareMode.LENIENT;

  private static final String MAGIC_NUMBER = "99911999";
  private static final String MAGIC_DATE = "2020-09-09T09:09:09.090Z";

  public Payload(String payload) {
    this.payload = payload;
  }

  public static Payload fromJson(@Language("json") String json) {
    return new Payload(json);
  }

  public static Payload fromFile(String path) {
    var resource = new ClassPathResource(path);
    try (var inputStream = new FileInputStream(resource.getFile())) {
      return fromJson(new String(inputStream.readAllBytes(), Charset.defaultCharset()));
    } catch (IOException e) {
      throw new RuntimeException("Failed to load payload from file '%s'".formatted(path), e);
    }
  }

  public Payload withId(String id) {
    with("id", id);
    return this;
  }

  public Payload withTimestamp(String timestamp) {
    with("timestamp", timestamp);
    return this;
  }

  public Payload withTimestamp(Instant timestamp) {
    return withTimestamp(timestamp.toString());
  }

  public Payload with(String key, Object value) {
    if (!payload.contains(toPlaceholder(key))) {
      throw new AssertionError(
          "\n❌\tCan not find placeholder '%s' in payload:\n%s"
              .formatted(toPlaceholder(key), payload));
    }
    payload = payload.replaceAll("\\$\\{" + key + "}", value.toString());
    return this;
  }

  public Payload lenient() {
    compareMode = JSONCompareMode.LENIENT;
    return this;
  }

  public Payload strict() {
    compareMode = JSONCompareMode.STRICT;
    return this;
  }

  public Payload strictOrder() {
    compareMode = JSONCompareMode.STRICT_ORDER;
    return this;
  }

  public Payload nonExtensible() {
    compareMode = JSONCompareMode.NON_EXTENSIBLE;
    return this;
  }

  public Payload ignorePlaceholders(String... placeholders) {
    this.ignoredPlaceholders =
        Arrays.stream(placeholders)
            .map(
                ignoredPlaceholder ->
                    !ignoredPlaceholder.equals(MAGIC_DATE)
                            && !ignoredPlaceholder.equals(MAGIC_NUMBER)
                            && !containsPlaceholder(ignoredPlaceholder)
                        ? toPlaceholder(ignoredPlaceholder)
                        : ignoredPlaceholder)
            .toList();
    return this;
  }

  private static String toPlaceholder(String ignoredPlaceholder) {
    return "${" + ignoredPlaceholder + "}";
  }

  public String toString() {
    var keyValueMap = jsonToKeyValueMap(payload);
    var unusedPlaceholders =
        keyValueMap.entrySet().stream()
            .filter(entry -> containsPlaceholder(entry.getValue()))
            .filter(entry -> !containsIgnoredPlaceholder(entry.getValue()))
            .map(entry -> entry.getKey() + ": " + entry.getValue())
            .sorted()
            .collect(Collectors.joining("\n\t🚫\t"));
    if (!unusedPlaceholders.isEmpty()) {
      throw new AssertionError("\n❌\tFound unused placeholders:\n\t🚫\t" + unusedPlaceholders);
    }

    return payload;
  }

  private static boolean containsPlaceholder(String text) {
    return text.contains("${") && text.contains("}")
        || text.contains(MAGIC_NUMBER)
        || text.contains(MAGIC_DATE);
  }

  private boolean containsIgnoredPlaceholder(String text) {
    return ignoredPlaceholders != null && ignoredPlaceholders.stream().anyMatch(text::contains);
  }

  private Map<String, String> jsonToKeyValueMap(String json) {
    try {
      JsonNode node = new ObjectMapper().readTree(json);
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
