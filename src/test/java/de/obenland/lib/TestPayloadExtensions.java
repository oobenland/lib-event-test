package de.obenland.lib;

import de.obenland.lib.eventtest.Payload;

public class TestPayloadExtensions {
  public static Payload withCustom(Payload payload, String custom) {
    return payload.with("custom", custom);
  }
}
