package org.hypertrace.entity.query.service.converter.filter;

import static org.hypertrace.core.documentstore.expression.operators.LogicalOperator.AND;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.quality.Strictness.LENIENT;

import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.Operator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class LogicalExpressionConverterTest {
  @Mock private FilterConverterFactory filterConverterFactory;
  @Mock private Converter<Filter, RelationalExpression> relationalExpressionConverter;
  @Mock private FilterTypeExpression filteringExpression1;
  @Mock private FilterTypeExpression filteringExpression2;

  private final Filter childFilter1 = Filter.newBuilder().setOperator(Operator.IN).build();
  private final Filter childFilter2 = Filter.newBuilder().setOperator(Operator.EQ).build();
  private final Filter filter =
      Filter.newBuilder()
          .setOperator(Operator.AND)
          .addChildFilter(childFilter1)
          .addChildFilter(childFilter2)
          .build();
  private final RequestContext requestContext = RequestContext.forTenantId("Martian");

  private Converter<Filter, FilterTypeExpression> logicalExpressionConverter;

  @BeforeEach
  void setup() throws ConversionException {
    logicalExpressionConverter = new LogicalExpressionConverter(filterConverterFactory);

    doReturn(relationalExpressionConverter)
        .when(filterConverterFactory)
        .getFilterConverter(any(Operator.class));
    doReturn(filteringExpression1)
        .when(relationalExpressionConverter)
        .convert(childFilter1, requestContext);
    doReturn(filteringExpression2)
        .when(relationalExpressionConverter)
        .convert(childFilter2, requestContext);
  }

  @Test
  void testConvert() throws ConversionException {
    LogicalExpression expected =
        LogicalExpression.builder()
            .operator(AND)
            .operand(filteringExpression1)
            .operand(filteringExpression2)
            .build();
    assertEquals(expected, logicalExpressionConverter.convert(filter, requestContext));
  }

  @Test
  void testConvertInvalidOperator() {
    Filter filter =
        Filter.newBuilder()
            .setOperator(Operator.LT)
            .addChildFilter(childFilter1)
            .addChildFilter(childFilter2)
            .build();
    assertThrows(
        ConversionException.class,
        () -> logicalExpressionConverter.convert(filter, requestContext));
  }

  @Test
  void testEmptyChildFilters() {
    Filter filter = Filter.newBuilder().setOperator(Operator.AND).build();
    assertThrows(
        ConversionException.class,
        () -> logicalExpressionConverter.convert(filter, requestContext));
  }

  @Test
  void testSingleChildFilter() throws ConversionException {
    Filter filter =
        Filter.newBuilder().setOperator(Operator.AND).addChildFilter(childFilter1).build();
    assertEquals(filteringExpression1, logicalExpressionConverter.convert(filter, requestContext));
  }

  @Test
  void testAllChildFiltersEmpty() {
    Filter filter =
        Filter.newBuilder()
            .setOperator(Operator.OR)
            .addChildFilter(Filter.getDefaultInstance())
            .build();
    assertThrows(
        ConversionException.class,
        () -> logicalExpressionConverter.convert(filter, requestContext));
  }

  @Test
  void testFewChildFiltersEmpty() throws ConversionException {
    Filter filter =
        Filter.newBuilder()
            .setOperator(Operator.OR)
            .addChildFilter(Filter.getDefaultInstance())
            .addChildFilter(childFilter1)
            .build();
    assertEquals(filteringExpression1, logicalExpressionConverter.convert(filter, requestContext));
  }
}
