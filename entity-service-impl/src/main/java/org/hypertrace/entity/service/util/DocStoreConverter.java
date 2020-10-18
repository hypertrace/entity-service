package org.hypertrace.entity.service.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.entity.data.service.v1.AttributeFilter;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValue.TypeCase;
import org.hypertrace.entity.data.service.v1.Operator;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityServiceConstants;

/**
 * Contains utility functions to transform object/DTOs between EDS(EntityDefinitionService) and
 * DocStore
 */
public class DocStoreConverter {

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static DocStoreJsonFormat.Printer JSONFORMAT_PRINTER = DocStoreJsonFormat.printer();
  private static final String ATTRIBUTES_LABELS_FIELD_NAME = "attributes.labels";
  private static final String VALUE_LIST_VALUES_CONST = ".valueList.values";

  /**
   * Transforms entity to JSONDocument
   *
   * @param entity
   * @return
   */
  public static <T extends GeneratedMessageV3> JSONDocument transform(T entity) throws IOException {
    // We need to use patched json converter because
    // the one from protobuf serializes 64 bit numbers into strings.
    // See https://github.com/protocolbuffers/protobuf/issues/1823
    String json = JSONFORMAT_PRINTER.print(entity);

    return new JSONDocument(json);
  }

  public static org.hypertrace.core.documentstore.Query transform(@Nonnull String tenantId,
      @Nonnull Query query) {
    org.hypertrace.core.documentstore.Query docStoreQuery = new org.hypertrace.core.documentstore.Query();

    List<Filter> filters = new ArrayList<>();
    filters.add(getTenantIdEqFilter(tenantId));

    if (!query.getEntityIdList().isEmpty()) {
      filters
          .add(new Filter(Filter.Op.IN, EntityServiceConstants.ENTITY_ID, query.getEntityIdList()));
    }
    if (StringUtils.isNotEmpty(query.getEntityType())) {
      filters
          .add(new Filter(Filter.Op.EQ, EntityServiceConstants.ENTITY_TYPE, query.getEntityType()));
    }
    if (StringUtils.isNotEmpty(query.getEntityName())) {
      filters
          .add(new Filter(Filter.Op.EQ, EntityServiceConstants.ENTITY_NAME, query.getEntityName()));
    }
    if (query.hasFilter()) {
      filters.add(transform(query.getFilter()));
    }
    if (!filters.isEmpty()) {
      if (filters.size() == 1) {
        docStoreQuery.setFilter(filters.get(0));
      } else {
        Filter f = new Filter();
        f.setOp(Filter.Op.AND);
        f.setChildFilters(filters.toArray(new Filter[]{}));
        docStoreQuery.setFilter(f);
      }
    }
    return docStoreQuery;
  }

  public static Filter getTenantIdEqFilter(String tenantId) {
    return new Filter(Filter.Op.EQ, EntityServiceConstants.TENANT_ID, tenantId);
  }

  private static Filter transform(AttributeFilter filter) {
    try {
      Filter f = new Filter();
      f.setFieldName(filter.getName());
      f.setOp(transform(filter.getOperator()));
      if (filter.hasAttributeValue()) {
        //transform(filter.getAttributeValue(), f);
        if (ATTRIBUTES_LABELS_FIELD_NAME.equals(f.getFieldName()) && f.getOp() == Op.EQ) {
          transformToEqFilterWithValueListRhs(filter.getAttributeValue(), f);
        } else if (ATTRIBUTES_LABELS_FIELD_NAME.equals(f.getFieldName()) && f.getOp() == Op.IN) {
          transformToOrFilterChainForStrArray(filter.getAttributeValue(), f);
          // Early return on this one since the childFilters are in the Or chain.
          return f;
        } else {
          transform(filter.getAttributeValue(), f, isPartOfAttributeMap(f.getFieldName()));
        }
      }

      f.setChildFilters(
          filter.getChildFilterList().stream()
              .map(DocStoreConverter::transform)
              .collect(Collectors.toList())
              .toArray(new Filter[]{}));
      return f;
    } catch (IOException ioe) {
      throw new IllegalArgumentException("Error converting filter for query");
    }
  }

  private static boolean isPartOfAttributeMap(String fieldName) {
    return !EntityServiceConstants.ENTITY_CREATED_TIME.equalsIgnoreCase(fieldName);
  }

//  private static void transform(AttributeValue attributeValue, Filter filter) throws IOException {
//    if (ATTRIBUTES_LABELS_FIELD_NAME.equals(filter.getFieldName()) && filter.getOp() == Op.EQ) {
//      transformToEqFilterWithValueListRhs(attributeValue, filter);
//    } else if (ATTRIBUTES_LABELS_FIELD_NAME.equals(filter.getFieldName()) && filter.getOp() == Op.IN) {
//      transformToOrFilterChainForStrArray(attributeValue, filter);
//    } else {
//      transform(attributeValue, filter, isPartOfAttributeMap(filter.getFieldName()));
//    }
//  }

  private static void transformToOrFilterChainForStrArray(AttributeValue attributeValue, Filter filter) {
    String fieldName = filter.getFieldName() + VALUE_LIST_VALUES_CONST;

    filter.setFieldName("");
    filter.setOp(Op.OR);
    filter.setChildFilters(
        attributeValue.getValueList().getValuesList().stream()
            .map(rhsAttributeValue -> {
              Filter f = new Filter();
              f.setFieldName(fieldName);
              f.setOp(Op.EQ);

              JsonNode mapNode = null;
              try {
                mapNode = OBJECT_MAPPER
                    .readTree(JSONFORMAT_PRINTER.print(rhsAttributeValue));
              } catch (JsonProcessingException | InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
              }
              Map map = OBJECT_MAPPER.convertValue(mapNode, Map.class);
              f.setValue(map);
              return f;
            })
            .collect(Collectors.toList())
            .toArray(new Filter[]{})
    );
  }

  private static void transformToEqFilterWithValueListRhs(AttributeValue attributeValue, Filter filter)
      throws InvalidProtocolBufferException, JsonProcessingException {
    String fieldName = filter.getFieldName() + VALUE_LIST_VALUES_CONST;
    filter.setFieldName(fieldName);

    org.hypertrace.entity.data.service.v1.AttributeValue.TypeCase typeCase = attributeValue.getTypeCase();
    if (typeCase == TypeCase.VALUE) {
      JsonNode mapNode = OBJECT_MAPPER
          .readTree(JSONFORMAT_PRINTER.print(attributeValue));
      Map map = OBJECT_MAPPER.convertValue(mapNode, Map.class);
      filter.setValue(map);
    } else { // TODO: Handle VALUE_LIST specially as well. Will consult if this is needed.
      throw new UnsupportedOperationException(
          String.format("The RHS of filter for string array types can only be VALUE or VALUE_LIST: %s", attributeValue));
    }
  }

  private static void transform(AttributeValue attributeValue, Filter filter,
      boolean isAttributeField) throws IOException {
    switch (attributeValue.getTypeCase()) {
      case VALUE: {
        Value value = attributeValue.getValue();
        String fieldName = filter.getFieldName();
        if (isAttributeField) {
          fieldName = filter.getFieldName()
              + "." + "value"
              + "." + value.getTypeCase().name().toLowerCase();
        }
        filter.setFieldName(fieldName);
        filter.setValue(getValue(value));
      }
      break;
      case VALUE_LIST: {
        filter.setFieldName(createFieldNameForValueList(attributeValue, filter, isAttributeField));
        if (filter.getOp().equals(Op.CONTAINS)) {
          JsonNode mapNode = OBJECT_MAPPER
              .readTree(JSONFORMAT_PRINTER.print(attributeValue.getValue()));
          Map map = OBJECT_MAPPER.convertValue(mapNode, Map.class);
          filter.setValue(map);
        } else if (filter.getOp().equals(Filter.Op.EQ)) {
          List<Object> listNodes = new ArrayList<>();
          for (AttributeValue v : attributeValue.getValueList().getValuesList()) {
            listNodes.add(OBJECT_MAPPER
                .convertValue(OBJECT_MAPPER.readTree(JSONFORMAT_PRINTER.print(v)), Map.class));
          }
          filter.setValue(listNodes);
        } else if (filter.getOp().equals(Op.IN)) {
          List<Object> listNodes = new ArrayList<>();
          for (AttributeValue v : attributeValue.getValueList().getValuesList()) {
            listNodes.add(getValue(v.getValue()));
          }
          filter.setValue(listNodes);
        } else {
          throw new UnsupportedOperationException(
              "Only CONTAINS, EQ and IN conditions supported for attribute values of type list");
        }
      }
      break;
      case VALUE_MAP: {
        if (filter.getOp().equals(Filter.Op.EQ)) {
          String fieldName = filter.getFieldName();
          if (isAttributeField) {
            fieldName = filter.getFieldName()
                + "." + "valueMap";
          }
          filter.setFieldName(fieldName);
          JsonNode mapNode = OBJECT_MAPPER
              .readTree(JSONFORMAT_PRINTER.print(attributeValue.getValueMap()));
          Map map = OBJECT_MAPPER.convertValue(mapNode, Map.class);
          filter.setValue(map);
        } else {
          throw new UnsupportedOperationException(
              "Only EQ condition supported for attribute values of type map");
        }
      }
      break;
    }
  }

  private static String createFieldNameForValueList(AttributeValue attributeValue, Filter filter,
      boolean isAttributeField) {
    if (!isAttributeField) {
      return filter.getFieldName();
    }

    if (filter.getOp().equals(Op.IN)) {
      Optional<String> fieldNameSuffix = attributeValue.getValueList().getValuesList().stream()
          .findFirst().map((attr) ->
              attr.getValue().getTypeCase().name().toLowerCase());
      return filter.getFieldName()
          + "." + "value"
          + "." + fieldNameSuffix.orElse("string");
    } else {
      return filter.getFieldName() + VALUE_LIST_VALUES_CONST;
    }
  }

  private static Object getValue(Value value) {
    switch (value.getTypeCase()) {
      case INT:
        return value.getInt();
      case LONG:
        return value.getLong();
      case FLOAT:
        return value.getFloat();
      case BYTES:
        return value.getBytes();
      case DOUBLE:
        return value.getDouble();
      case STRING:
        return value.getString();
      case BOOLEAN:
        return value.getBoolean();
      case TIMESTAMP:
        return value.getTimestamp();
      case CUSTOM:
        return value.getCustom().toString();
      case TYPE_NOT_SET:
      default:
        throw new IllegalArgumentException("Filter value not set");
    }
  }

  private static Filter.Op transform(Operator operator) {
    switch (operator) {
      case AND:
        return Op.AND;
      case OR:
        return Op.OR;
      case EQ:
        return Op.EQ;
      case NEQ:
        return Op.NEQ;
      case IN:
        return Op.IN;
      case GT:
        return Op.GT;
      case LT:
        return Op.LT;
      case GE:
        return Op.GTE;
      case LE:
        return Op.LTE;
      case CONTAINS:
        return Op.CONTAINS;
      case LIKE:
        return Op.LIKE;
      case NOT_IN:
      default:
        throw new IllegalArgumentException(
            String.format("Operator conversion is not supported for: %s", operator));
    }
  }
}
