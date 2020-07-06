package org.hypertrace.entity.service.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class StringUtilsTest {

  @Test
  public void testEmptyAndNonEmptyStringChecks() {
    Assertions.assertTrue(StringUtils.isEmpty(null));
    Assertions.assertTrue(StringUtils.isEmpty(""));
    Assertions.assertFalse(StringUtils.isEmpty("foo"));
    Assertions.assertFalse(StringUtils.isEmpty(" "));

    Assertions.assertFalse(StringUtils.isNotEmpty(null));
    Assertions.assertFalse(StringUtils.isNotEmpty(""));
    Assertions.assertTrue(StringUtils.isNotEmpty("foo"));
    Assertions.assertTrue(StringUtils.isNotEmpty(" "));
  }
}
