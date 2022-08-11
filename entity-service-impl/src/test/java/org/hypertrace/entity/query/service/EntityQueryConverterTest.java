package org.hypertrace.entity.query.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.common.EntityAttributeMapping;
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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EntityQueryConverterTest {
  private static final String ATTRIBUTE_ID_NAME1 = "eqsColumn1";
  private static final String EDS_COLUMN_NAME1 = "edsColumn1";
  private static final String ATTRIBUTE_ID_NAME2 = "eqsColumn2";
  private static final String EDS_COLUMN_NAME2 = "edsColumn2";

  @Mock EntityAttributeMapping mockAttributeMapping;
  @Mock RequestContext mockRequestContext;
  private EntityQueryConverter queryConverter;

  @BeforeEach
  public void setup() {
    this.queryConverter = new EntityQueryConverter(mockAttributeMapping);
  }

  @Test
  public void test_convertToEDSQuery_limitAndOffset() {
    // no offset and limit specified
    EntityQueryRequest request = EntityQueryRequest.newBuilder().build();
    Query convertedQuery = this.queryConverter.convertToEDSQuery(mockRequestContext, request);
    assertEquals(0, convertedQuery.getOffset());
    assertEquals(0, convertedQuery.getLimit());

    int limit = 3;
    int offset = 1;
    request = EntityQueryRequest.newBuilder().setLimit(limit).setOffset(offset).build();
    convertedQuery = this.queryConverter.convertToEDSQuery(mockRequestContext, request);
    assertEquals(limit, convertedQuery.getLimit());
    assertEquals(offset, convertedQuery.getOffset());
  }

  @Test
  public void test_convertToEdsQuery_orderByExpression() {
    mockAttribute1();
    mockAttribute2();
    EntityQueryRequest request =
        EntityQueryRequest.newBuilder()
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setExpression(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(ATTRIBUTE_ID_NAME1)
                                    .build()))
                    .setOrder(SortOrder.ASC)
                    .build())
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setExpression(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(ATTRIBUTE_ID_NAME2)
                                    .build()))
                    .setOrder(SortOrder.DESC)
                    .build())
            .build();
    Query convertedQuery = queryConverter.convertToEDSQuery(mockRequestContext, request);
    assertEquals(2, convertedQuery.getOrderByCount());
    // ensure that the order of OrderByExpression is maintained
    assertEquals(EDS_COLUMN_NAME1, convertedQuery.getOrderByList().get(0).getName());
    assertEquals(
        org.hypertrace.entity.data.service.v1.SortOrder.ASC,
        convertedQuery.getOrderBy(0).getOrder());
    assertEquals(EDS_COLUMN_NAME2, convertedQuery.getOrderByList().get(1).getName());
    assertEquals(
        org.hypertrace.entity.data.service.v1.SortOrder.DESC,
        convertedQuery.getOrderBy(1).getOrder());
  }

  @Test
  public void test_convertToEdsQuery_missingSortOrderByExpression_assignWithAscByDefault() {
    mockAttribute1();
    EntityQueryRequest request =
        EntityQueryRequest.newBuilder()
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setExpression(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(ATTRIBUTE_ID_NAME1)
                                    .build())))
            .build();
    Query convertedQuery = queryConverter.convertToEDSQuery(mockRequestContext, request);
    assertEquals(1, convertedQuery.getOrderByCount());
    // ensure that the order of OrderByExpression is maintained
    assertEquals(EDS_COLUMN_NAME1, convertedQuery.getOrderByList().get(0).getName());
    assertEquals(
        org.hypertrace.entity.data.service.v1.SortOrder.ASC,
        convertedQuery.getOrderBy(0).getOrder());
  }

  @Test
  public void test_filter() {
    mockAttribute2();
    EntityQueryRequest queryRequest =
        EntityQueryRequest.newBuilder()
            .setEntityType(EntityType.SERVICE.name())
            .setFilter(
                Filter.newBuilder()
                    .setOperatorValue(Operator.LT.getNumber())
                    .setLhs(
                        Expression.newBuilder()
                            .setColumnIdentifier(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(ATTRIBUTE_ID_NAME2)
                                    .build())
                            .build())
                    .setRhs(
                        Expression.newBuilder()
                            .setLiteral(
                                LiteralConstant.newBuilder()
                                    .setValue(
                                        org.hypertrace.entity.query.service.v1.Value.newBuilder()
                                            .setLong(Instant.now().toEpochMilli())
                                            .setValueType(ValueType.LONG)
                                            .build())
                                    .build())
                            .build())
                    .build())
            .build();
    Query query = queryConverter.convertToEDSQuery(mockRequestContext, queryRequest);
    Assertions.assertEquals(EntityType.SERVICE.name(), query.getEntityType());
    Assertions.assertNotNull(query.getFilter());
  }

  @Test
  public void test_convertToEdsQuery_functionExpressionOrderByExpression_throwsException() {
    EntityQueryRequest request =
        EntityQueryRequest.newBuilder()
            .addOrderBy(
                OrderByExpression.newBuilder()
                    .setExpression(
                        Expression.newBuilder().setFunction(Function.newBuilder().build()).build())
                    .setOrder(SortOrder.ASC)
                    .build())
            .build();
    assertThrows(
        UnsupportedOperationException.class,
        () -> queryConverter.convertToEDSQuery(mockRequestContext, request));
  }

  private void mockAttribute1() {
    when(mockAttributeMapping.getDocStorePathByAttributeId(mockRequestContext, ATTRIBUTE_ID_NAME1))
        .thenReturn(Optional.of(EDS_COLUMN_NAME1));
  }

  private void mockAttribute2() {
    when(mockAttributeMapping.getDocStorePathByAttributeId(mockRequestContext, ATTRIBUTE_ID_NAME2))
        .thenReturn(Optional.of(EDS_COLUMN_NAME2));
  }
}
