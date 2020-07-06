package org.hypertrace.entity.service.util;

import com.github.f4b6a3.uuid.UuidCreator;
import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;

/**
 * Helper class to generate {@link UUID} based on the identifying attributes of an {@link Entity}
 */
public class UUIDGenerator {

  //A randomly generated UUID required for the UUID(type 5) generation
  //TODO: Need to see if this should be dynamically generated based on the Tenant ID
  private static final UUID NAMESPACE_UUID = UUID
      .fromString("5088c92d-5e9c-43f4-a35b-2589474d5642");

  // version bits for UUID 3
  private static final long VERSION_BITS = 3 << 12;

  /**
   * <b>IMPORTANT: This is to be used only by the Entity Service.</b>
   * This ID generation is bound to change and no consumer can make an assumption that this will be
   * the UUID generation logic for ever
   */
  public static String generateUUID(Map<String, AttributeValue> attributes) {
    if (attributes.isEmpty()) {
      return java.util.UUID.randomUUID().toString();
    }
    return getUUIDWithVersion3(transform(attributes).toString()).toString();
  }

  /**
   * Explicitly set the version of UUID to UUIDv3
   * <p>
   * Priori, we were using UUID implementation from http://commons.apache.org/sandbox/commons-id/source-repository.html
   * for generating UUID using SHA-1 and with a namespace using method nameUUIDFromString. It has an
   * issue, as per rfc4122, the version would have been 5, but it generates UUID with version 3. To
   * support backward compatibility with our data, we decided to continue resetting the UUID version
   * to 3.
   */
  private static UUID getUUIDWithVersion3(String name) {
    UUID uuid5 = UuidCreator.getNameBasedSha1(NAMESPACE_UUID, name);
    long msb3 = (uuid5.getMostSignificantBits() & 0xffffffffffff0fffL) | VERSION_BITS;
    return new UUID(msb3, uuid5.getLeastSignificantBits());
  }

  @VisibleForTesting
  static Map<String, Object> transform(Map<String, AttributeValue> input) {
    Map<String, Object> output = new TreeMap<>(input);
    for (Map.Entry<String, AttributeValue> entry : input.entrySet()) {
      AttributeValue value = entry.getValue();
      switch (value.getTypeCase()) {
        case VALUE:
          output.put(entry.getKey(), value.getValue());
          break;
        case VALUE_LIST:
          output.put(entry.getKey(), transform(value.getValueList().getValuesList()));
          break;
        case VALUE_MAP:
          output.put(entry.getKey(), transform(value.getValueMap().getValuesMap()));
          break;
      }
    }
    return output;
  }

  private static List<Object> transform(List<AttributeValue> attributeValueList) {
    if (attributeValueList.isEmpty()) {
      return null;
    }
    //Assuming uniform type for all values in the AttributeList. So get type from the first one
    AttributeValue.TypeCase attributeValueType = attributeValueList.get(0).getTypeCase();
    switch (attributeValueType) {
      case VALUE:
        return attributeValueList.stream()
            .map(AttributeValue::getValue)
            .sorted(new AttributeListComparator())
            .collect(Collectors.toList());
      case VALUE_LIST:
        throw new IllegalArgumentException(
            "List of Lists is not supported in identifying attributes of an Entity");
      case VALUE_MAP:
        throw new IllegalArgumentException(
            "List of Maps is not supported in identifying attributes of an Entity");
    }
    return null;
  }

  private static class AttributeListComparator implements Comparator<Value> {

    @Override
    public int compare(Value v1, Value v2) {
      if (!v1.getTypeCase().equals(v2.getTypeCase())) {
        throw new IllegalArgumentException(
            "Heterogeneous lists are not supported in identifying attributes of an Entity");
      }
      Value.TypeCase valueType = v1.getTypeCase();
      switch (valueType) {
        case STRING:
          return v1.getString().compareTo(v2.getString());
        case TIMESTAMP:
          return Long.compare(v1.getTimestamp(), v2.getTimestamp());
        case BOOLEAN:
          return Boolean.compare(v1.getBoolean(), v2.getBoolean());
        case DOUBLE:
          return Double.compare(v1.getDouble(), v2.getDouble());
        case BYTES:
          return ByteString.unsignedLexicographicalComparator()
              .compare(v1.getBytes(), v2.getBytes());
        case FLOAT:
          return Float.compare(v1.getFloat(), v2.getFloat());
        case LONG:
          return Long.compare(v1.getLong(), v2.getLong());
        case INT:
          return Integer.compare(v1.getInt(), v2.getInt());
        case CUSTOM:
          throw new IllegalArgumentException(
              "Custom value types are not supported in identitying attributes of an Entity");
      }
      return 0;
    }
  }
}
