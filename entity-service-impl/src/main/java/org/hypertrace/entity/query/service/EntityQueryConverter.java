package org.hypertrace.entity.query.service;

import static java.util.stream.Collectors.toMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
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

class EntityQueryConverter {

  private final EntityAttributeMapping attributeMapping;

  EntityQueryConverter(EntityAttributeMapping attributeMapping) {
    this.attributeMapping = attributeMapping;
  }

  @Deprecated(forRemoval = true)
  public Query convertToEDSQuery(RequestContext requestContext, EntityQueryRequest queryRequest) {
    Query.Builder queryBuilder = Query.newBuilder().setEntityType(queryRequest.getEntityType());

    AttributeFilter attributeFilter =
        convertToAttributeFilter(requestContext, queryBuilder, queryRequest.getFilter());
    if (attributeFilter != null) {
      queryBuilder.setFilter(attributeFilter);
    }

    queryBuilder.addAllOrderBy(convertOrderBy(requestContext, queryRequest.getOrderByList()));
    queryBuilder.setLimit(queryRequest.getLimit());
    queryBuilder.setOffset(queryRequest.getOffset());

    return queryBuilder.build();
  }

  private AttributeFilter convertToAttributeFilter(
      RequestContext requestContext, Query.Builder queryBuilder, Filter filter) {
    if (filter == null || filter.equals(Filter.getDefaultInstance())) {
      return null;
    }
    AttributeFilter.Builder builder = null;
    if (filter.getChildFilterCount() == 0) {
      // Copy the lhs and rhs from the filter.
      AttributeColumnInformation attributeData =
          getAttributeColumnInfo(requestContext, filter.getLhs());
      String edsColumnName = attributeData.getColumnName();
      org.hypertrace.entity.query.service.v1.Value rhsValue =
          filter.getRhs().getLiteral().getValue();
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
        builder.setIsMultiValued(attributeData.isMultiValued());
        builder.setAttributeValue(convertToAttributeValue(filter.getRhs()));
      }
    } else {
      builder = AttributeFilter.newBuilder();
      builder.setOperator(convertOperator(filter.getOperator()));
      for (Filter child : filter.getChildFilterList()) {
        AttributeFilter attributeFilter =
            convertToAttributeFilter(requestContext, queryBuilder, child);
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

  private static AttributeValue.Builder convertToAttributeValue(Expression expression) {
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

  private AttributeColumnInformation getAttributeColumnInfo(
      RequestContext requestContext, Expression expression) {
    switch (expression.getValueCase()) {
      case LITERAL:
        throw new IllegalArgumentException("LHS should be a Attribute key");
      case COLUMNIDENTIFIER:
        String attributeId = expression.getColumnIdentifier().getColumnName();
        String attributeName =
            this.attributeMapping
                .getDocStorePathByAttributeId(requestContext, attributeId)
                .orElseThrow(
                    () ->
                        new IllegalArgumentException(
                            String.format(
                                "Unrecognized attribute: %s does not match any known entity attribute",
                                attributeId)));
        final Optional<AttributeKind> attributeKind =
            this.attributeMapping.getAttributeKind(requestContext, attributeId);
        final boolean isArray =
            attributeKind.isPresent() && this.attributeMapping.isArray(attributeKind.get());
        return new AttributeColumnInformation(attributeName, isArray);
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

  private List<org.hypertrace.entity.data.service.v1.OrderByExpression> convertOrderBy(
      RequestContext requestContext, List<OrderByExpression> orderByExpressions) {
    if (orderByExpressions.isEmpty()) {
      return Collections.emptyList();
    }

    List<org.hypertrace.entity.data.service.v1.OrderByExpression> result = new ArrayList<>();
    for (OrderByExpression orderByExpression : orderByExpressions)
      if (orderByExpression.hasExpression()) {
        if (orderByExpression.getExpression().hasColumnIdentifier()) {
          String edsColumnName =
              getAttributeColumnInfo(requestContext, orderByExpression.getExpression())
                  .getColumnName();
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
    return SortOrder.DESC == sortOrder
        ? org.hypertrace.entity.data.service.v1.SortOrder.DESC
        : org.hypertrace.entity.data.service.v1.SortOrder.ASC;
  }

  @Value
  private static class AttributeColumnInformation {
    String columnName;
    boolean multiValued;
  }
}
