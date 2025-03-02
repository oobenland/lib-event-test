package de.obenland.lib;

import de.obenland.lib.eventtest.Payload;

import java.util.List;

public class TestPayloadExtensions {
  public static Payload withCustom(Payload self, String custom) {
    return self.with("custom", custom);
  }

  public static Payload withCustomArray(Payload self, String jsonPath, List<String> values) {
    return self.withArray(jsonPath, values, Payload::withId);
  }
}
