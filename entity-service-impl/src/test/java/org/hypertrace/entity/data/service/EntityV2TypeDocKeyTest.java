package org.hypertrace.entity.data.service;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.hypertrace.core.documentstore.Key;
import org.junit.jupiter.api.Test;

class EntityV2TypeDocKeyTest {

  @Test
  void usesAllFieldsInGeneratingString() {
    Key key = new EntityV2TypeDocKey("AAA", "BBB", "CCC");

    assertEquals("AAA:BBB:CCC", key.toString());
  }
}
