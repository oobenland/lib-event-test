package it.obenland.lib;

import it.obenland.lib.eventtest.EventPayload;

import java.util.List;

public class TestPayloadExtensions {
  public static EventPayload withCustom(EventPayload self, String custom) {
    return self.with("custom", custom);
  }

  public static EventPayload withCustomArray(EventPayload self, String jsonPath, List<String> values) {
    return self.withArray(jsonPath, values, EventPayload::withId);
  }
}
