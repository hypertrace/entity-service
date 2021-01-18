package org.hypertrace.entity.query.service;

import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hypertrace.entity.data.service.v1.AttributeFilter;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.AttributeValueMap;
import org.hypertrace.entity.data.service.v1.Operator;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.OrderByExpression;
import org.hypertrace.entity.query.service.v1.SortOrder;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.constants.EntityServiceConstants;

public class EntityQueryConverter {

  public static Query convertToEDSQuery(EntityQueryRequest queryRequest,
      Map<String, String> attrNameToEDSAttrMap) {
    Query.Builder queryBuilder = Query.newBuilder()
        .setEntityType(queryRequest.getEntityType());

    AttributeFilter attributeFilter = convertToAttributeFilter(queryBuilder,
        queryRequest.getFilter(), attrNameToEDSAttrMap);
    if (attributeFilter != null) {
      queryBuilder.setFilter(attributeFilter);
    }

    queryBuilder.addAllOrderBy(convertOrderBy(queryRequest.getOrderByList(), attrNameToEDSAttrMap));
    queryBuilder.setLimit(queryRequest.getLimit());
    queryBuilder.setOffset(queryRequest.getOffset());

    return queryBuilder.build();
  }

  public static org.hypertrace.entity.query.service.v1.Value convertAttributeValueToQueryValue(
      AttributeValue attributeValue) {
    if (attributeValue == null) {
      return org.hypertrace.entity.query.service.v1.Value.getDefaultInstance();
    }
    switch (attributeValue.getTypeCase()) {
      case VALUE:
        return convertValueToQueryValue(attributeValue.getValue());
      case VALUE_LIST:
        //TODO for now converting everything to string array
        return org.hypertrace.entity.query.service.v1.Value.newBuilder()
            .setValueType(ValueType.STRING_ARRAY)
            .addAllStringArray(attributeValue.getValueList().getValuesList()
                .stream().map(EntityQueryConverter::toStringWithoutTypeInfo)
                .collect(Collectors.toList()))
            .build();
      case VALUE_MAP:
        //TODO for now converting everything to string map
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, AttributeValue> entry :
            attributeValue.getValueMap().getValuesMap().entrySet()) {
          map.put(entry.getKey(), toStringWithoutTypeInfo(entry.getValue()));
        }
        return org.hypertrace.entity.query.service.v1.Value.newBuilder()
            .setValueType(ValueType.STRING_MAP)
            .putAllStringMap(map).build();
      case TYPE_NOT_SET:
      default:
        return org.hypertrace.entity.query.service.v1.Value.getDefaultInstance();
    }
  }

  public static List<String> convertSelectionsToDocStoreSelections(
      List<Expression> expressions, Map<String, String> attrNameToEDSAttrMap) {
    if (expressions.isEmpty()) {
      return Collections.emptyList();
    }

    List<String> result = new ArrayList<>();
    for (Expression expression : expressions) {
      if (expression.hasColumnIdentifier()) {
        String docStoreColumnName =
            EntityQueryConverter.convertToAttributeKey(expression, attrNameToEDSAttrMap);
        result.add(docStoreColumnName);
      } else {
        // entity data service and doc store only support field selection. There's no
        // aggregate selection yet
        throw new UnsupportedOperationException(
            "Expression only support Column Identifier Expression");
      }
    }
    return result;
  }

  private static org.hypertrace.entity.query.service.v1.Value convertValueToQueryValue(
      org.hypertrace.entity.data.service.v1.Value value) {
    switch (value.getTypeCase()) {
      case STRING:
        return org.hypertrace.entity.query.service.v1.Value.newBuilder()
            .setValueType(ValueType.STRING)
            .setString(value.getString()).build();
      case BOOLEAN:
        return org.hypertrace.entity.query.service.v1.Value.newBuilder()
            .setValueType(ValueType.BOOL)
            .setBoolean(value.getBoolean()).build();
      case INT:
        return org.hypertrace.entity.query.service.v1.Value.newBuilder()
            .setValueType(ValueType.INT)
            .setInt(value.getInt()).build();
      case LONG:
        return org.hypertrace.entity.query.service.v1.Value.newBuilder()
            .setValueType(ValueType.LONG)
            .setLong(value.getLong()).build();
      case FLOAT:
        return org.hypertrace.entity.query.service.v1.Value.newBuilder()
            .setValueType(ValueType.FLOAT)
            .setFloat(value.getFloat()).build();
      case DOUBLE:
        return org.hypertrace.entity.query.service.v1.Value.newBuilder()
            .setValueType(ValueType.DOUBLE)
            .setDouble(value.getDouble()).build();
      case BYTES:
        return org.hypertrace.entity.query.service.v1.Value.newBuilder()
            .setValueType(ValueType.BYTES)
            .setBytes(value.getBytes()).build();
      case TIMESTAMP:
        return org.hypertrace.entity.query.service.v1.Value.newBuilder()
            .setValueType(ValueType.TIMESTAMP)
            .setTimestamp(value.getTimestamp()).build();
      case TYPE_NOT_SET:
      case CUSTOM:
      default:
        throw new IllegalStateException("Unexpected value: " + value.getTypeCase());
    }
  }

  private static AttributeFilter convertToAttributeFilter(
      Query.Builder queryBuilder,
      Filter filter,
      Map<String, String> attrNameToEDSAttrMap) {
    if (filter == null || filter.equals(Filter.getDefaultInstance())) {
      return null;
    }
    AttributeFilter.Builder builder = null;
    if (filter.getChildFilterCount() == 0) {
      // Copy the lhs and rhs from the filter.
      String edsColumnName = convertToAttributeKey(filter.getLhs(), attrNameToEDSAttrMap);
      org.hypertrace.entity.query.service.v1.Value rhsValue = filter.getRhs().getLiteral()
          .getValue();
      if (edsColumnName.equals(EntityServiceConstants.ENTITY_ID)) {
        if (rhsValue.getValueType() == ValueType.STRING) {
          queryBuilder.addEntityId(rhsValue.getString());
        } else if (rhsValue.getValueType() == ValueType.STRING_ARRAY) {
          queryBuilder.addAllEntityId(rhsValue.getStringArrayList());
        }
      } else if (edsColumnName.equals(EntityServiceConstants.ENTITY_NAME)) {
        queryBuilder.setEntityName(rhsValue.getString());
      } else {
        builder = AttributeFilter.newBuilder();
        builder.setOperator(convertOperator(filter.getOperator()));
        builder.setName(edsColumnName);
        builder.setAttributeValue(convertToAttributeValue(filter.getRhs()));
      }
    } else {
      builder = AttributeFilter.newBuilder();
      builder.setOperator(convertOperator(filter.getOperator()));
      for (Filter child : filter.getChildFilterList()) {
        AttributeFilter attributeFilter = convertToAttributeFilter(
            queryBuilder, child, attrNameToEDSAttrMap);
        if (null != attributeFilter) {
          builder.addChildFilter(attributeFilter);
        }
      }
    }
    return (null != builder) ? builder.build() : null;
  }

  private static org.hypertrace.entity.data.service.v1.Operator convertOperator(
      org.hypertrace.entity.query.service.v1.Operator operator) {
    // Might not handle all cases but this is a starter.
    return Operator.valueOf(operator.name());
  }

  private static AttributeValue.Builder convertToAttributeValue(
      Expression expression) {
    switch (expression.getValueCase()) {
      case LITERAL:
        return convertToAttributeValue(expression.getLiteral());
      case COLUMNIDENTIFIER:
        throw new IllegalArgumentException("Column Identifier should be LHS");
      case FUNCTION:
        throw new UnsupportedOperationException(
            "Filtering on functional expressions not supported in EDS");
      case ORDERBY:
        throw new IllegalArgumentException("Filter cant have a Order By clause");
      case VALUE_NOT_SET:
      default:
        return AttributeValue.newBuilder();
    }
  }

  public static AttributeValue.Builder convertToAttributeValue(
      LiteralConstant literal) {
    AttributeValue.Builder builder = AttributeValue.newBuilder();
    org.hypertrace.entity.query.service.v1.Value value = literal.getValue();
    switch (literal.getValue().getValueType()) {
      case UNRECOGNIZED:
        return null;
      case BOOL:
        builder.setValue(org.hypertrace.entity.data.service.v1.Value.newBuilder()
            .setBoolean(value.getBoolean()));
        break;
      case STRING:
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder().setString(value.getString()));
        break;
      case LONG:
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder().setLong(value.getLong()));
        break;
      case TIMESTAMP:
        builder.setValue(org.hypertrace.entity.data.service.v1.Value.newBuilder()
            .setTimestamp(value.getTimestamp()));
        break;
      case DOUBLE:
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder().setDouble(value.getDouble()));
        break;
      case FLOAT:
        builder.setValue(
            org.hypertrace.entity.data.service.v1.Value.newBuilder().setFloat(value.getFloat())
                .build());
        break;
      case BOOLEAN_ARRAY:
        builder.setValueList(AttributeValueList.newBuilder()
            .addAllValues(value.getBooleanArrayList().stream()
                .map(x -> AttributeValue.newBuilder()
                    .setValue(
                        org.hypertrace.entity.data.service.v1.Value.newBuilder().setBoolean(x))
                    .build())
                .collect(Collectors.toList())))
            .build();
        break;
      case STRING_ARRAY:
        builder.setValueList(AttributeValueList.newBuilder()
            .addAllValues(value.getStringArrayList().stream()
                .map(x -> AttributeValue.newBuilder()
                    .setValue(org.hypertrace.entity.data.service.v1.Value.newBuilder().setString(x))
                    .build())
                .collect(Collectors.toList())))
            .build();
        break;
      case LONG_ARRAY:
        builder.setValueList(AttributeValueList.newBuilder()
            .addAllValues(value.getLongArrayList().stream()
                .map(x -> AttributeValue.newBuilder()
                    .setValue(org.hypertrace.entity.data.service.v1.Value.newBuilder().setLong(x))
                    .build())
                .collect(Collectors.toList())))
            .build();
        break;
      case DOUBLE_ARRAY:
        builder.setValueList(AttributeValueList.newBuilder()
            .addAllValues(value.getDoubleArrayList().stream()
                .map(x -> AttributeValue.newBuilder()
                    .setValue(org.hypertrace.entity.data.service.v1.Value.newBuilder().setDouble(x))
                    .build())
                .collect(Collectors.toList())))
            .build();
        break;
      case STRING_MAP:
        builder.setValueMap(AttributeValueMap.newBuilder()
            .putAllValues(value.getStringMapMap().entrySet()
                .stream()
                .collect(toMap(
                    Entry::getKey,
                    val -> AttributeValue.newBuilder()
                        .setValue(org.hypertrace.entity.data.service.v1.Value.newBuilder()
                            .setString(val.getValue())
                            .build())
                        .build())))
            .build());
        break;
    }
    return builder;
  }

  private static String convertToAttributeKey(
      Expression expression,
      Map<String, String> attrNameToEDSAttrMap) {
    switch (expression.getValueCase()) {
      case LITERAL:
        throw new IllegalArgumentException("LHS should be a Attribute key");
      case COLUMNIDENTIFIER:
        return attrNameToEDSAttrMap.get(expression.getColumnIdentifier().getColumnName());
      case FUNCTION:
        throw new UnsupportedOperationException(
            "Filtering on functional expressions not supported in EDS");
      case ORDERBY:
        throw new IllegalArgumentException("Filter cant have a Order By clause");
      case VALUE_NOT_SET:
      default:
        throw new IllegalArgumentException(
            String.format("Attribute Key for expression:%s not found", expression));
    }
  }

  /**
   * Method to return the String representation of the given attribute without including any type
   * information.
   */
  private static String toStringWithoutTypeInfo(AttributeValue value) {
    switch (value.getTypeCase()) {
      case VALUE:
        org.hypertrace.entity.data.service.v1.Value inner = value.getValue();
        switch (inner.getTypeCase()) {
          case INT:
            return String.valueOf(inner.getInt());
          case LONG:
            return String.valueOf(inner.getLong());
          case FLOAT:
            return String.valueOf(inner.getFloat());
          case DOUBLE:
            return String.valueOf(inner.getDouble());
          case TIMESTAMP:
            return String.valueOf(inner.getTimestamp());
          case STRING:
            return inner.getString();
          case BYTES:
            return String.valueOf(inner.getBytes());
          case BOOLEAN:
            return String.valueOf(inner.getBoolean());
        }
        throw new IllegalArgumentException("Unhandled type: " + inner.getTypeCase());

      case VALUE_LIST:
        String[] values = new String[value.getValueList().getValuesCount()];
        for (int i = 0; i < value.getValueList().getValuesCount(); i++) {
          values[i] = toStringWithoutTypeInfo(value.getValueList().getValues(i));
        }
        return Arrays.toString(values);
    }
    throw new IllegalArgumentException("Unhandled type: " + value.getTypeCase());
  }

  private static List<org.hypertrace.entity.data.service.v1.OrderByExpression> convertOrderBy(
      List<OrderByExpression> orderByExpressions,
      Map<String, String> attrNameToEDSAttrMap) {
    if (orderByExpressions.isEmpty()) {
      return Collections.emptyList();
    }

    List<org.hypertrace.entity.data.service.v1.OrderByExpression> result = new ArrayList<>();
    for (OrderByExpression orderByExpression : orderByExpressions)
      if (orderByExpression.hasExpression()) {
        if (orderByExpression.getExpression().hasColumnIdentifier()) {
          String edsColumnName = convertToAttributeKey(
              orderByExpression.getExpression(), attrNameToEDSAttrMap);
          org.hypertrace.entity.data.service.v1.OrderByExpression convertedExpression =
              org.hypertrace.entity.data.service.v1.OrderByExpression.newBuilder()
                  .setName(edsColumnName)
                  .setOrder(convertSortOrder(orderByExpression.getOrder()))
                  .build();
          result.add(convertedExpression);
        } else {
          // entity data service and doc store only support field order by. There's no
          // aggregate order by yet
          throw new UnsupportedOperationException(
              "OrderByExpression only support Column Identifier Expression");
        }
      }
    return result;
  }

  private static org.hypertrace.entity.data.service.v1.SortOrder convertSortOrder(
      SortOrder sortOrder) {
    return SortOrder.DESC == sortOrder ?
        org.hypertrace.entity.data.service.v1.SortOrder.DESC :
        org.hypertrace.entity.data.service.v1.SortOrder.ASC;
  }
}
