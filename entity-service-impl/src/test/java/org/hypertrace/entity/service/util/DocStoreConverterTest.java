package org.hypertrace.entity.service.util;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.OrderBy;
import org.hypertrace.entity.data.service.v1.AttributeFilter;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Operator;
import org.hypertrace.entity.data.service.v1.OrderByExpression;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.data.service.v1.SortOrder;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.service.constants.EntityConstants;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Tests the utility functions in {@link DocStoreConverter}
 */
public class DocStoreConverterTest {

  private static final String TENANT_ID = "__default";
  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static DocStoreJsonFormat.Printer JSONFORMAT_PRINTER = DocStoreJsonFormat.printer();
  private static final String ATTRIBUTES_LABELS_FIELD_NAME = "attributes.labels";


  @Test
  public void testEntityQueryLimitOffsetConversion() {
    int limit = 2;
    int offset = 1;
    Query query = Query.newBuilder().addEntityId("some id").build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());
    Assertions.assertNull(transformedQuery.getLimit());
    Assertions.assertNull(transformedQuery.getOffset());

    query = Query.newBuilder().addEntityId("some id").setLimit(limit).setOffset(offset).build();
    transformedQuery = DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());
    Assertions.assertEquals(limit, transformedQuery.getLimit());
    Assertions.assertEquals(offset, transformedQuery.getOffset());

    // zero values will be ignored
    query = Query.newBuilder().addEntityId("some id").setLimit(0).setOffset(0).build();
    transformedQuery = DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());
    Assertions.assertNull(transformedQuery.getLimit());
    Assertions.assertNull(transformedQuery.getOffset());
  }

  @Test
  public void testEntityFieldsQueryConversion() {
    Query query = Query.newBuilder().addEntityId("some id").build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());
    Filter transformedFilter = transformedQuery.getFilter();

    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());

    // Verify that the first filter is based on tenant id.
    Filter tenantIdFilter = transformedFilter.getChildFilters()[0];

    Assertions.assertEquals(Op.EQ, tenantIdFilter.getOp());
    Assertions.assertEquals(TENANT_ID, tenantIdFilter.getValue());
    Assertions.assertEquals(EntityServiceConstants.TENANT_ID, tenantIdFilter.getFieldName());
    Assertions.assertEquals(EntityServiceConstants.ENTITY_ID,
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Collections.singletonList("some id"),
        transformedFilter.getChildFilters()[1].getValue());
    Assertions.assertEquals(Filter.Op.IN, transformedFilter.getChildFilters()[1].getOp());

    query = Query.newBuilder().addEntityId("some id").setEntityName("some name").build();

    transformedQuery = DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());
    transformedFilter = transformedQuery.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());
    Assertions.assertEquals(3, transformedFilter.getChildFilters().length);
    Assertions.assertEquals(EntityServiceConstants.ENTITY_ID,
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Collections.singletonList("some id"),
        transformedFilter.getChildFilters()[1].getValue());
    Assertions.assertEquals(Filter.Op.IN, transformedFilter.getChildFilters()[1].getOp());
    Assertions.assertEquals(EntityServiceConstants.ENTITY_NAME,
        transformedFilter.getChildFilters()[2].getFieldName());
    Assertions.assertEquals("some name", transformedFilter.getChildFilters()[2].getValue());
    Assertions.assertEquals(Filter.Op.EQ, transformedFilter.getChildFilters()[2].getOp());
  }

  @Test
  public void testAttributeValueConversion() {
    Query query = Query.newBuilder()
        .setFilter(AttributeFilter.newBuilder()
            .setName(EntityConstants.ATTRIBUTES_MAP_PATH)
            .setOperator(Operator.CONTAINS)
            .setAttributeValue(AttributeValue.newBuilder()
                .setValue(Value.newBuilder()
                    .setString("stringValue")
                    .build())
                .build())
            .build())
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());

    Filter transformedFilter = transformedQuery.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());

    Assertions.assertEquals("attributes.value.string",
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Filter.Op.CONTAINS, transformedFilter.getChildFilters()[1].getOp());
    Assertions.assertEquals("stringValue", transformedFilter.getChildFilters()[1].getValue());
  }

  @Test
  public void testEntityFieldAndAttributeValueConversion() {
    Query query = Query.newBuilder()
        .addEntityId("some id")
        .setFilter(AttributeFilter.newBuilder()
            .setName(EntityConstants.ATTRIBUTES_MAP_PATH)
            .setOperator(Operator.CONTAINS)
            .setAttributeValue(AttributeValue.newBuilder()
                .setValue(Value.newBuilder()
                    .setString("stringValue")
                    .build())
                .build())
            .build())
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());

    Filter transformedFilter = transformedQuery.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());

    Assertions.assertEquals(3, transformedFilter.getChildFilters().length);
    Assertions.assertEquals(EntityServiceConstants.ENTITY_ID,
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Collections.singletonList("some id"),
        transformedFilter.getChildFilters()[1].getValue());
    Assertions.assertEquals("attributes.value.string",
        transformedFilter.getChildFilters()[2].getFieldName());
    Assertions.assertEquals(Filter.Op.CONTAINS, transformedFilter.getChildFilters()[2].getOp());
    Assertions.assertEquals("stringValue", transformedFilter.getChildFilters()[2].getValue());
  }


  @Test
  public void testFilterNonAttributesFieldNames() {
    Query query = Query.newBuilder()
        .addEntityId("some id")
        .setFilter(AttributeFilter.newBuilder()
            .setName("API.createdTime")
            .setOperator(Operator.GT)
            .setAttributeValue(AttributeValue.newBuilder()
                .setValue(Value.newBuilder().setLong(1234L).build())
                .build())
            .build())
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());

    Filter transformedFilter = transformedQuery.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());

    Assertions.assertEquals(3, transformedFilter.getChildFilters().length);
    Assertions.assertEquals(EntityServiceConstants.ENTITY_ID,
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Collections.singletonList("some id"),
        transformedFilter.getChildFilters()[1].getValue());
    Assertions.assertEquals("API.createdTime",
        transformedFilter.getChildFilters()[2].getFieldName());
    Assertions.assertEquals(Filter.Op.GT, transformedFilter.getChildFilters()[2].getOp());
    Assertions.assertEquals(1234L, transformedFilter.getChildFilters()[2].getValue());
  }

  @Test
  public void testOrderByConversion() {
    Query query = Query.newBuilder()
        .addEntityId("some id")
        .addOrderBy(
            OrderByExpression.newBuilder()
                .setName("col1")
                .setOrder(SortOrder.DESC)
                .build())
        .addOrderBy(
            OrderByExpression.newBuilder()
                .setName("col2")
                .build())
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());

    Assertions.assertEquals(0, transformedQuery.getSelections().size());

    List<OrderBy> transformedOrderBys = transformedQuery.getOrderBys();

    Assertions.assertEquals(2, transformedOrderBys.size());
    Assertions.assertEquals("col1", transformedOrderBys.get(0).getField());
    Assertions.assertFalse(transformedOrderBys.get(0).isAsc());
    Assertions.assertEquals("col2", transformedOrderBys.get(1).getField());
    Assertions.assertTrue(transformedOrderBys.get(1).isAsc());
  }

  @Test
  public void testSelectionsWithOrderBy() {
    Query query = Query.newBuilder()
        .addEntityId("some id")
        .addOrderBy(
            OrderByExpression.newBuilder()
                .setName("col1")
                .setOrder(SortOrder.DESC)
                .build())
        .addOrderBy(
            OrderByExpression.newBuilder()
                .setName("col2")
                .build())
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, List.of("selection1", "selection2"));
    List<String> selections = transformedQuery.getSelections();
    Assertions.assertEquals(2, selections.size());
    Assertions.assertEquals("selection1", selections.get(0));
    Assertions.assertEquals("selection2", selections.get(1));

    List<OrderBy> transformedOrderBys = transformedQuery.getOrderBys();

    Assertions.assertEquals(2, transformedOrderBys.size());
    Assertions.assertEquals("col1", transformedOrderBys.get(0).getField());
    Assertions.assertFalse(transformedOrderBys.get(0).isAsc());
    Assertions.assertEquals("col2", transformedOrderBys.get(1).getField());
    Assertions.assertTrue(transformedOrderBys.get(1).isAsc());
  }

  @Test
  public void testInFilterConversion() {
    Query query = Query.newBuilder()
        .setFilter(AttributeFilter.newBuilder()
            .setName(EntityConstants.ATTRIBUTES_MAP_PATH)
            .setOperator(Operator.IN)
            .setAttributeValue(AttributeValue.newBuilder()
                .setValueList(
                    AttributeValueList.newBuilder()
                        .addValues(AttributeValue.newBuilder()
                            .setValue(Value.newBuilder().setString("attr-v1")))
                        .addValues(AttributeValue.newBuilder()
                            .setValue(Value.newBuilder().setString("attr-v2")))
                        .addValues(AttributeValue.newBuilder()
                            .setValue(Value.newBuilder().setString("attr-v3")))
                )
                .build())
            .build())
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());

    Filter transformedFilter = transformedQuery.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());

    Assertions.assertEquals(2, transformedFilter.getChildFilters().length);
    Assertions.assertEquals("attributes.value.string",
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Filter.Op.IN, transformedFilter.getChildFilters()[1].getOp());
    Assertions.assertEquals(List.of("attr-v1", "attr-v2", "attr-v3"),
        transformedFilter.getChildFilters()[1].getValue());

    // Empty value list. Default to string attribute value type for the field name
    Query query2 = Query.newBuilder()
        .setFilter(AttributeFilter.newBuilder()
            .setName(EntityConstants.ATTRIBUTES_MAP_PATH)
            .setOperator(Operator.IN)
            .setAttributeValue(AttributeValue.newBuilder()
                .setValueList(AttributeValueList.newBuilder())
                .build())
            .build())
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery2 =
        DocStoreConverter.transform(TENANT_ID, query2, Collections.emptyList());

    Filter transformedFilter2 = transformedQuery2.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter2.getOp());

    Assertions.assertEquals(2, transformedFilter2.getChildFilters().length);
    Assertions.assertEquals("attributes.value.string",
        transformedFilter2.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Filter.Op.IN, transformedFilter2.getChildFilters()[1].getOp());
    Assertions.assertEquals(List.of(), transformedFilter2.getChildFilters()[1].getValue());

    // Long list
    Query query3 = Query.newBuilder()
        .setFilter(AttributeFilter.newBuilder()
            .setName(EntityConstants.ATTRIBUTES_MAP_PATH)
            .setOperator(Operator.IN)
            .setAttributeValue(AttributeValue.newBuilder()
                .setValueList(
                    AttributeValueList.newBuilder()
                        .addValues(
                            AttributeValue.newBuilder().setValue(Value.newBuilder().setLong(21L)))
                        .addValues(
                            AttributeValue.newBuilder().setValue(Value.newBuilder().setLong(22L)))
                )
                .build())
            .build())
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery3 =
        DocStoreConverter.transform(TENANT_ID, query3, Collections.emptyList());

    Filter transformedFilter3 = transformedQuery3.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter3.getOp());

    Assertions.assertEquals(2, transformedFilter3.getChildFilters().length);
    Assertions.assertEquals("attributes.value.long",
        transformedFilter3.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Filter.Op.IN, transformedFilter3.getChildFilters()[1].getOp());
    Assertions.assertEquals(List.of(21L, 22L), transformedFilter3.getChildFilters()[1].getValue());

    // CreatedTime field. Will not be part of attributes and so no need to specify the type suffix
    Query query4 = Query.newBuilder()
        .setFilter(AttributeFilter.newBuilder()
            .setName(EntityServiceConstants.ENTITY_CREATED_TIME)
            .setOperator(Operator.IN)
            .setAttributeValue(AttributeValue.newBuilder()
                .setValueList(
                    AttributeValueList.newBuilder()
                        .addValues(
                            AttributeValue.newBuilder().setValue(Value.newBuilder().setLong(21L)))
                        .addValues(
                            AttributeValue.newBuilder().setValue(Value.newBuilder().setLong(22L)))
                )
                .build())
            .build())
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery4 =
        DocStoreConverter.transform(TENANT_ID, query4, Collections.emptyList());

    Filter transformedFilter4 = transformedQuery4.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter4.getOp());

    Assertions.assertEquals(2, transformedFilter4.getChildFilters().length);
    Assertions.assertEquals("createdTime", transformedFilter4.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Filter.Op.IN, transformedFilter4.getChildFilters()[1].getOp());
    Assertions.assertEquals(List.of(21L, 22L), transformedFilter4.getChildFilters()[1].getValue());
  }

  @Test
  public void testEqFilterConversionForValueList() throws IOException {
    Query query = Query.newBuilder()
        .setFilter(AttributeFilter.newBuilder()
            .setName(EntityConstants.ATTRIBUTES_MAP_PATH)
            .setOperator(Operator.EQ)
            .setAttributeValue(AttributeValue.newBuilder()
                .setValueList(
                    AttributeValueList.newBuilder()
                        .addValues(AttributeValue.newBuilder()
                            .setValue(Value.newBuilder().setString("{\"a1\":\"v1\"}")))
                        .addValues(AttributeValue.newBuilder()
                            .setValue(Value.newBuilder().setString("{\"a2\":\"v2\"}")))
                        .addValues(AttributeValue.newBuilder()
                            .setValue(Value.newBuilder().setString("{\"a3\":\"v3\"}")))
                )
                .build())
            .build())
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());

    Filter transformedFilter = transformedQuery.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());

    Assertions.assertEquals(2, transformedFilter.getChildFilters().length);
    Assertions.assertEquals("attributes.valueList.values",
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Filter.Op.EQ, transformedFilter.getChildFilters()[1].getOp());
    // I am not sure what if the value conversion is correct for this but will not modify it this time. Will leave and test
    // the logic as is for now.
    Assertions.assertEquals(
        List.of(
            OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree(JSONFORMAT_PRINTER.print(
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("{\"a1\":\"v1\"}")))), Map.class),
            OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree(JSONFORMAT_PRINTER.print(
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("{\"a2\":\"v2\"}")))), Map.class),
            OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree(JSONFORMAT_PRINTER.print(
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("{\"a3\":\"v3\"}")))), Map.class)
        ),
        transformedFilter.getChildFilters()[1].getValue()
    );
  }

  @Test
  public void testStringArrayValueTypeColumnEq() throws JsonProcessingException {
    Query query = Query.newBuilder()
        .addEntityId("some id")
        .setFilter(AttributeFilter.newBuilder()
            .setName(ATTRIBUTES_LABELS_FIELD_NAME)
            .setOperator(Operator.EQ)
            .setAttributeValue(AttributeValue.newBuilder()
                .setValue(Value.newBuilder().setString("l1"))
            )
        )
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());

    Filter transformedFilter = transformedQuery.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());

    Assertions.assertEquals(3, transformedFilter.getChildFilters().length);
    Assertions.assertEquals(EntityServiceConstants.ENTITY_ID,
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Collections.singletonList("some id"),
        transformedFilter.getChildFilters()[1].getValue());
    Assertions.assertEquals(ATTRIBUTES_LABELS_FIELD_NAME + ".valueList.values",
        transformedFilter.getChildFilters()[2].getFieldName());
    Assertions.assertEquals(Op.EQ, transformedFilter.getChildFilters()[2].getOp());
    Assertions.assertEquals(OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree("{\"value\": {\"string\":\"l1\"}}"), Map.class),
        transformedFilter.getChildFilters()[2].getValue());
  }

  @Test
  public void testStringArrayValueTypeColumnEqAndChain() throws JsonProcessingException {
    Query query = Query.newBuilder()
        .addEntityId("some id")
        .setFilter(
            AttributeFilter.newBuilder().setOperator(Operator.AND)
                .addChildFilter(
                    AttributeFilter.newBuilder()
                        .setName(ATTRIBUTES_LABELS_FIELD_NAME)
                        .setOperator(Operator.EQ)
                        .setAttributeValue(AttributeValue.newBuilder()
                            .setValue(Value.newBuilder().setString("l1"))
                        )
                )
                .addChildFilter(
                    AttributeFilter.newBuilder()
                        .setName(ATTRIBUTES_LABELS_FIELD_NAME)
                        .setOperator(Operator.EQ)
                        .setAttributeValue(AttributeValue.newBuilder()
                            .setValue(Value.newBuilder().setString("l2"))
                        )
                )
        )
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());

    Filter transformedFilter = transformedQuery.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());

    Assertions.assertEquals(3, transformedFilter.getChildFilters().length);
    Assertions.assertEquals(EntityServiceConstants.ENTITY_ID,
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Collections.singletonList("some id"),
        transformedFilter.getChildFilters()[1].getValue());

    Assertions.assertEquals(Op.AND, transformedFilter.getChildFilters()[2].getOp());

    Assertions.assertEquals(ATTRIBUTES_LABELS_FIELD_NAME + ".valueList.values",
        transformedFilter.getChildFilters()[2].getChildFilters()[0].getFieldName());
    Assertions.assertEquals(Op.EQ, transformedFilter.getChildFilters()[2].getChildFilters()[0].getOp());
    Assertions.assertEquals(OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree("{\"value\": {\"string\":\"l1\"}}"), Map.class),
        transformedFilter.getChildFilters()[2].getChildFilters()[0].getValue());

    Assertions.assertEquals(ATTRIBUTES_LABELS_FIELD_NAME + ".valueList.values",
        transformedFilter.getChildFilters()[2].getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Op.EQ, transformedFilter.getChildFilters()[2].getChildFilters()[1].getOp());
    Assertions.assertEquals(OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree("{\"value\": {\"string\":\"l2\"}}"), Map.class),
        transformedFilter.getChildFilters()[2].getChildFilters()[1].getValue());
  }

  @Test
  public void testStringArrayValueTypeColumnOrChain() throws JsonProcessingException {
    Query query = Query.newBuilder()
        .addEntityId("some id")
        .setFilter(
            AttributeFilter.newBuilder().setOperator(Operator.AND)
                .addChildFilter(
                    AttributeFilter.newBuilder()
                        .setName("attributes.some_col")
                        .setOperator(Operator.EQ)
                        .setAttributeValue(AttributeValue.newBuilder()
                            .setValue(Value.newBuilder().setString("some_val"))
                        )
                )
                .addChildFilter(
                    AttributeFilter.newBuilder()
                        .setName(ATTRIBUTES_LABELS_FIELD_NAME)
                        .setOperator(Operator.IN)
                        .setAttributeValue(AttributeValue.newBuilder()
                            .setValueList(
                                AttributeValueList.newBuilder()
                                    .addValues(
                                        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l1"))
                                    )
                                    .addValues(
                                        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l2"))
                                    )
                                    .addValues(
                                        AttributeValue.newBuilder().setValue(Value.newBuilder().setString("l3"))
                                    )
                            )
                        )
                )
        )
        .build();
    org.hypertrace.core.documentstore.Query transformedQuery =
        DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());

    Filter transformedFilter = transformedQuery.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());

    Assertions.assertEquals(3, transformedFilter.getChildFilters().length);
    Assertions.assertEquals(EntityServiceConstants.ENTITY_ID,
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Collections.singletonList("some id"),
        transformedFilter.getChildFilters()[1].getValue());

    Assertions.assertEquals(Op.AND, transformedFilter.getChildFilters()[2].getOp());

    Assertions.assertEquals("attributes.some_col.value.string",
        transformedFilter.getChildFilters()[2].getChildFilters()[0].getFieldName());
    Assertions.assertEquals(Op.EQ, transformedFilter.getChildFilters()[2].getChildFilters()[0].getOp());
    Assertions.assertEquals("some_val", transformedFilter.getChildFilters()[2].getChildFilters()[0].getValue());

    Assertions.assertEquals(Op.OR, transformedFilter.getChildFilters()[2].getChildFilters()[1].getOp());

    Assertions.assertEquals(ATTRIBUTES_LABELS_FIELD_NAME + ".valueList.values",
        transformedFilter.getChildFilters()[2].getChildFilters()[1].getChildFilters()[0].getFieldName());
    Assertions.assertEquals(Op.EQ, transformedFilter.getChildFilters()[2].getChildFilters()[1].getChildFilters()[0].getOp());
    Assertions.assertEquals(OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree("{\"value\": {\"string\":\"l1\"}}"), Map.class),
        transformedFilter.getChildFilters()[2].getChildFilters()[1].getChildFilters()[0].getValue());

    Assertions.assertEquals(ATTRIBUTES_LABELS_FIELD_NAME + ".valueList.values",
        transformedFilter.getChildFilters()[2].getChildFilters()[1].getChildFilters()[1].getFieldName());
    Assertions.assertEquals(Op.EQ, transformedFilter.getChildFilters()[2].getChildFilters()[1].getChildFilters()[1].getOp());
    Assertions.assertEquals(OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree("{\"value\": {\"string\":\"l2\"}}"), Map.class),
        transformedFilter.getChildFilters()[2].getChildFilters()[1].getChildFilters()[1].getValue());

    Assertions.assertEquals(ATTRIBUTES_LABELS_FIELD_NAME + ".valueList.values",
        transformedFilter.getChildFilters()[2].getChildFilters()[1].getChildFilters()[2].getFieldName());
    Assertions.assertEquals(Op.EQ, transformedFilter.getChildFilters()[2].getChildFilters()[1].getChildFilters()[2].getOp());
    Assertions.assertEquals(OBJECT_MAPPER.convertValue(OBJECT_MAPPER.readTree("{\"value\": {\"string\":\"l3\"}}"), Map.class),
        transformedFilter.getChildFilters()[2].getChildFilters()[1].getChildFilters()[2].getValue());
  }

  @Test
  public void testNeqFilterConversionForValueListThrowsException() {
    Assertions.assertThrows(UnsupportedOperationException.class, () -> {
      Query query = Query.newBuilder()
          .setFilter(AttributeFilter.newBuilder()
              .setName(EntityConstants.ATTRIBUTES_MAP_PATH)
              .setOperator(Operator.NEQ)
              .setAttributeValue(AttributeValue.newBuilder()
                  .setValueList(
                      AttributeValueList.newBuilder()
                          .addValues(AttributeValue.newBuilder()
                              .setValue(Value.newBuilder().setString("{\"a1\":\"v1\"}")))
                          .addValues(AttributeValue.newBuilder()
                              .setValue(Value.newBuilder().setString("{\"a2\":\"v2\"}")))
                          .addValues(AttributeValue.newBuilder()
                              .setValue(Value.newBuilder().setString("{\"a3\":\"v3\"}")))
                  )
                  .build())
              .build())
          .build();
      DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());
    });
  }

  @Test
  public void entityToJSONDocumentConversion64BitNumberFixup() throws IOException {
    // Verify that timestamp attribute which is a long number won't get
    // converted to string
    Entity testEntity =
        Entity.newBuilder()
            .setTenantId("tenant1")
            .setEntityName("myentity1")
            .putIdentifyingAttributes(
                "entity_id",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("my-entity-id-1"))
                    .build())
            .putAttributes(
                "timestamp",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setLong(1584055141072L))
                    .build())
            .build();

    JSONDocument jsonDocument = DocStoreConverter.transform(testEntity);
    Assertions.assertEquals(
        "{\"tenantId\":\"tenant1\",\"entityName\":\"myentity1\",\"identifyingAttributes\":{\"entity_id\":{\"value\":{\"string\":\"my-entity-id-1\"}}},\"attributes\":{\"timestamp\":{\"value\":{\"long\":1584055141072}}}}",
        jsonDocument.toJson());
  }

  @Test
  public void testFilterOpConversion() {
    Operator[] operators = new Operator[]{
        Operator.AND, Operator.OR, Operator.EQ, Operator.NEQ, Operator.IN, Operator.GT, Operator.LT,
        Operator.GE,
        Operator.LE, Operator.CONTAINS, Operator.LIKE
    };
    Filter.Op[] expectedFilterOps = new Filter.Op[]{
        Filter.Op.AND, Filter.Op.OR, Filter.Op.EQ, Filter.Op.NEQ, Filter.Op.IN, Filter.Op.GT,
        Filter.Op.LT, Filter.Op.GTE,
        Filter.Op.LTE, Filter.Op.CONTAINS, Filter.Op.LIKE
    };

    for (int i = 0; i < operators.length; i++) {
      testFilterOpConversionForEntityServiceOp(operators[i], expectedFilterOps[i]);
    }

    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      createAndTransformQuery(Operator.UNDEFINED);
    });
    // There's currently no op for NOT_IN
    Assertions.assertThrows(IllegalArgumentException.class, () -> {
      createAndTransformQuery(Operator.NOT_IN);
    });
  }

  private void testFilterOpConversionForEntityServiceOp(Operator operator,
      Filter.Op expectedFilterOp) {
    org.hypertrace.core.documentstore.Query transformedQuery = createAndTransformQuery(operator);
    Filter transformedFilter = transformedQuery.getFilter();
    Assertions.assertEquals(Filter.Op.AND, transformedFilter.getOp());

    Assertions.assertEquals(2, transformedFilter.getChildFilters().length);
    Assertions.assertEquals("attributes.value.string",
        transformedFilter.getChildFilters()[1].getFieldName());
    Assertions.assertEquals(expectedFilterOp, transformedFilter.getChildFilters()[1].getOp());
    Assertions.assertEquals("stringValue", transformedFilter.getChildFilters()[1].getValue());
  }

  private org.hypertrace.core.documentstore.Query createAndTransformQuery(Operator operator) {
    Query query = Query.newBuilder()
        .setFilter(AttributeFilter.newBuilder()
            .setName(EntityConstants.ATTRIBUTES_MAP_PATH)
            .setOperator(operator)
            .setAttributeValue(AttributeValue.newBuilder()
                .setValue(Value.newBuilder()
                    .setString("stringValue")
                    .build())
                .build())
            .build())
        .build();

    return DocStoreConverter.transform(TENANT_ID, query, Collections.emptyList());
  }
}

