package org.hypertrace.entity.query.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.Function;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.OrderByExpression;
import org.hypertrace.entity.query.service.v1.SortOrder;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.v1.entitytype.EntityType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class EntityQueryConverterTest {
  private static final String EQS_COLUMN_NAME1 = "eqsColumn1";
  private static final String EDS_COLUMN_NAME1 = "edsColumn1";
  private static final String EQS_COLUMN_NAME2 = "eqsColumn2";
  private static final String EDS_COLUMN_NAME2 = "edsColumn2";

  private final Map<String, String> attributeMap = new HashMap<>();

  @BeforeEach
  public void setup() {
    attributeMap.put(EQS_COLUMN_NAME1, EDS_COLUMN_NAME1);
    attributeMap.put(EQS_COLUMN_NAME2, EDS_COLUMN_NAME2);
  }

  @Test
  public void test_convertToEDSQuery_limitAndOffset() {
    // no offset and limit specified
    EntityQueryRequest request = EntityQueryRequest.newBuilder().build();
    Query convertedQuery = EntityQueryConverter.convertToEDSQuery(request, Collections.emptyMap());
    assertEquals(0, convertedQuery.getOffset());
    assertEquals(0, convertedQuery.getLimit());

    int limit = 3;
    int offset = 1;
    request = EntityQueryRequest.newBuilder().setLimit(limit).setOffset(offset).build();
    convertedQuery = EntityQueryConverter.convertToEDSQuery(request, Collections.emptyMap());
    assertEquals(limit, convertedQuery.getLimit());
    assertEquals(offset, convertedQuery.getOffset());
  }

  @Test
  public void test_convertToEdsQuery_orderByExpression() {
    EntityQueryRequest request = EntityQueryRequest.newBuilder()
        .addOrderBy(
            OrderByExpression.newBuilder()
                .setExpression(
                    Expression.newBuilder().setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                          .setColumnName(EQS_COLUMN_NAME1)
                          .build()))
                .setOrder(SortOrder.ASC)
            .build())
        .addOrderBy(
            OrderByExpression.newBuilder()
                .setExpression(
                    Expression.newBuilder().setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName(EQS_COLUMN_NAME2)
                            .build()))
                .setOrder(SortOrder.DESC)
                .build())
        .build();
    Query convertedQuery = EntityQueryConverter.convertToEDSQuery(request, attributeMap);
    assertEquals(2, convertedQuery.getOrderByCount());
    // ensure that the order of OrderByExpression is maintained
    assertEquals(EDS_COLUMN_NAME1, convertedQuery.getOrderByList().get(0).getName());
    assertEquals(org.hypertrace.entity.data.service.v1.SortOrder.ASC,
        convertedQuery.getOrderBy(0).getOrder());
    assertEquals(EDS_COLUMN_NAME2, convertedQuery.getOrderByList().get(1).getName());
    assertEquals(org.hypertrace.entity.data.service.v1.SortOrder.DESC,
        convertedQuery.getOrderBy(1).getOrder());

  }

  @Test
  public void test_convertToEdsQuery_missingSortOrderByExpression_assignWithAscByDefault() {
    EntityQueryRequest request = EntityQueryRequest.newBuilder()
        .addOrderBy(
            OrderByExpression.newBuilder()
                .setExpression(
                    Expression.newBuilder().setColumnIdentifier(
                        ColumnIdentifier.newBuilder()
                            .setColumnName(EQS_COLUMN_NAME1)
                            .build()))
        ).build();
    Query convertedQuery = EntityQueryConverter.convertToEDSQuery(request, attributeMap);
    assertEquals(1, convertedQuery.getOrderByCount());
    // ensure that the order of OrderByExpression is maintained
    assertEquals(EDS_COLUMN_NAME1, convertedQuery.getOrderByList().get(0).getName());
    assertEquals(org.hypertrace.entity.data.service.v1.SortOrder.ASC,
        convertedQuery.getOrderBy(0).getOrder());
  }

  @Test
  public void test_filter() {
    EntityQueryRequest queryRequest = EntityQueryRequest.newBuilder()
        .setEntityType(EntityType.SERVICE.name())
        .setFilter(
            Filter.newBuilder()
                .setOperatorValue(Operator.LT.getNumber())
                .setLhs(Expression.newBuilder().setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("SERVICE.createdTime").build()).build())
                .setRhs(Expression.newBuilder().setLiteral(
                    LiteralConstant.newBuilder().setValue(
                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                            .setLong(Instant.now().toEpochMilli())
                            .setValueType(ValueType.LONG)
                            .build())
                        .build()).
                    build())
                .build())
        .build();
    Query query = EntityQueryConverter.convertToEDSQuery(queryRequest, Map.of("SERVICE.createdTime", "createdTime"));
    Assertions.assertEquals(EntityType.SERVICE.name(), query.getEntityType());
    Assertions.assertNotNull(query.getFilter());
  }

  @Test
  public void test_convertToEdsQuery_functionExpressionOrderByExpression_throwsException() {
    EntityQueryRequest request = EntityQueryRequest.newBuilder()
        .addOrderBy(
            OrderByExpression.newBuilder()
                .setExpression(Expression.newBuilder()
                    .setFunction(
                        Function.newBuilder().build())
                    .build())
                .setOrder(SortOrder.ASC).build()
        ).build();
    assertThrows(UnsupportedOperationException.class,
        () -> EntityQueryConverter.convertToEDSQuery(request, Collections.emptyMap()));
  }

  @Test
  public void test_convertEqsSelections_DocStoreSelections() {
    List<Expression> expressions =
        List.of(
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName(EQS_COLUMN_NAME1).build())
                .build(),
            Expression.newBuilder()
                .setColumnIdentifier(
                    ColumnIdentifier.newBuilder().setColumnName(EQS_COLUMN_NAME2).build())
                .build());
    List<String> docStoreSelections =
        EntityQueryConverter.convertSelectionsToDocStoreSelections(expressions, attributeMap);
    assertEquals(2, docStoreSelections.size());
    assertEquals(EDS_COLUMN_NAME1, docStoreSelections.get(0));
    assertEquals(EDS_COLUMN_NAME2, docStoreSelections.get(1));
  }
}
