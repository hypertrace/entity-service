package org.hypertrace.entity.service.constants;

import com.google.protobuf.ProtocolMessageEnum;
import org.hypertrace.entity.constants.v1.EnumExtension;

/**
 * Constants used by the entities and entity service.
 */
public class EntityConstants {

  public static final String ATTRIBUTES_MAP_PATH = "attributes";

  public static String attributeMapPathFor(String attributeKey) {
    return String.join(".", ATTRIBUTES_MAP_PATH, attributeKey);
  }

  /**
   * Returns the constant value for the given Enum.
   *
   * @param key enum key defined in proto files.
   * @return the corresponding string value defined for that enum key.
   */
  public static String getValue(ProtocolMessageEnum key) {
    return key.getValueDescriptor().getOptions().getExtension(EnumExtension.stringValue);
  }
}
