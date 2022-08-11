package org.hypertrace.entity.service.util;

public class StringUtils {

  public static boolean isEmpty(String str) {
    return str == null || str.length() == 0;
  }

  public static boolean isNotEmpty(String str) {
    return !isEmpty(str);
  }

  public static boolean isBlank(final String str) {
    return str == null || isEmpty(str.trim());
  }

  public static boolean isNotBlank(final String str) {
    return !isBlank(str);
  }

  public static String removePrefix(String str, final String prefix) {
    if (str != null && prefix != null && str.startsWith(prefix)) {
      return str.substring(prefix.length());
    }
    return str;
  }
}
