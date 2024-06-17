package org.hypertrace.entity.query.service;

import static java.util.stream.Collectors.toMap;

import java.util.Map.Entry;
import java.util.stream.Collectors;
import lombok.NoArgsConstructor;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.AttributeValueMap;
import org.hypertrace.entity.query.service.v1.LiteralConstant;

@NoArgsConstructor
class EntityQueryConverter {

  public static AttributeValue.Builder convertToAttributeValue(LiteralConstant literal) {
    AttributeValue.Builder builder = AttributeValue.newBuilder();
    org.hypertrace.entity.query.service.v1.Value value = literal.getValue();
    switch (literal.getValue().getValueType()) {
      case UNRECOGNIZED:
        return null;
      case BOOL:
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                .setBoolean(value.getBoolean()));
        break;
      case STRING:
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder().setString(value.getString()));
        break;
      case INT: // Adding int conversion for backward compatibility.
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder().setInt(value.getInt()));
        break;
      case LONG:
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder().setLong(value.getLong()));
        break;
      case TIMESTAMP:
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                .setTimestamp(value.getTimestamp()));
        break;
      case DOUBLE:
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder().setDouble(value.getDouble()));
        break;
      case FLOAT:
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                .setFloat(value.getFloat())
                .build());
        break;
      case BOOLEAN_ARRAY:
        builder
            .setValueList(
                AttributeValueList.newBuilder()
                    .addAllValues(
                        value.getBooleanArrayList().stream()
                            .map(
                                x ->
                                    AttributeValue.newBuilder()
                                        .setValue(
                                            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                                                .setBoolean(x))
                                        .build())
                            .collect(Collectors.toList())))
            .build();
        break;
      case STRING_ARRAY:
        builder
            .setValueList(
                AttributeValueList.newBuilder()
                    .addAllValues(
                        value.getStringArrayList().stream()
                            .map(
                                x ->
                                    AttributeValue.newBuilder()
                                        .setValue(
                                            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                                                .setString(x))
                                        .build())
                            .collect(Collectors.toList())))
            .build();
        break;
      case LONG_ARRAY:
        builder
            .setValueList(
                AttributeValueList.newBuilder()
                    .addAllValues(
                        value.getLongArrayList().stream()
                            .map(
                                x ->
                                    AttributeValue.newBuilder()
                                        .setValue(
                                            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                                                .setLong(x))
                                        .build())
                            .collect(Collectors.toList())))
            .build();
        break;
      case DOUBLE_ARRAY:
        builder
            .setValueList(
                AttributeValueList.newBuilder()
                    .addAllValues(
                        value.getDoubleArrayList().stream()
                            .map(
                                x ->
                                    AttributeValue.newBuilder()
                                        .setValue(
                                            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                                                .setDouble(x))
                                        .build())
                            .collect(Collectors.toList())))
            .build();
        break;
      case STRING_MAP:
        builder.setValueMap(
            AttributeValueMap.newBuilder()
                .putAllValues(
                    value.getStringMapMap().entrySet().stream()
                        .collect(
                            toMap(
                                Entry::getKey,
                                val ->
                                    AttributeValue.newBuilder()
                                        .setValue(
                                            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                                                .setString(val.getValue())
                                                .build())
                                        .build())))
                .build());
        break;
    }
    return builder;
  }
}
