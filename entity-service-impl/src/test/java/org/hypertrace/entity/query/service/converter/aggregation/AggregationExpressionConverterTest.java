package org.hypertrace.entity.query.service.converter.aggregation;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.hypertrace.entity.query.service.v1.AggregationOperator.AGGREGATION_OPERATOR_DISTINCT_COUNT;
import static org.hypertrace.entity.query.service.v1.AggregationOperator.AGGREGATION_OPERATOR_UNSPECIFIED;
import static org.hypertrace.entity.query.service.v1.AggregationOperator.UNRECOGNIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.accessor.ExpressionOneOfAccessor;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
public class AggregationExpressionConverterTest {
  private Converter<org.hypertrace.entity.query.service.v1.AggregateExpression, AggregateExpression>
      aggregateExpressionConverter;

  @Mock private Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;
  @Mock private RequestContext requestContext;

  private final ColumnIdentifier columnIdentifier =
      ColumnIdentifier.newBuilder().setColumnName("Hello_Mars").build();
  private final IdentifierExpression identifierExpression = IdentifierExpression.of("Hello_Mars");

  private org.hypertrace.entity.query.service.v1.AggregateExpression.Builder
      aggregateExpressionBuilder;

  @BeforeEach
  void setup() throws ConversionException {
    OneOfAccessor<Expression, ValueCase> expressionAccessor = new ExpressionOneOfAccessor();
    aggregateExpressionConverter =
        new AggregateExpressionConverter(expressionAccessor, identifierExpressionConverter);
    aggregateExpressionBuilder =
        org.hypertrace.entity.query.service.v1.AggregateExpression.newBuilder()
            .setExpression(Expression.newBuilder().setColumnIdentifier(columnIdentifier));

    when(identifierExpressionConverter.convert(columnIdentifier, requestContext))
        .thenReturn(identifierExpression);
  }

  @ParameterizedTest
  @EnumSource(org.hypertrace.entity.query.service.v1.AggregationOperator.class)
  void testAggregationOperatorCoverage(
      final org.hypertrace.entity.query.service.v1.AggregationOperator operator)
      throws ConversionException {

    if (operator == UNRECOGNIZED) {
      return;
    }

    org.hypertrace.entity.query.service.v1.AggregateExpression expression =
        aggregateExpressionBuilder.setOperator(operator).build();

    if (operator == AGGREGATION_OPERATOR_UNSPECIFIED) {
      assertThrows(
          ConversionException.class,
          () -> aggregateExpressionConverter.convert(expression, requestContext));
    } else {
      assertNotNull(aggregateExpressionConverter.convert(expression, requestContext));
    }
  }

  @Test
  void testConvert() throws ConversionException {
    org.hypertrace.entity.query.service.v1.AggregateExpression expression =
        aggregateExpressionBuilder.setOperator(AGGREGATION_OPERATOR_DISTINCT_COUNT).build();
    AggregateExpression expected = AggregateExpression.of(DISTINCT_COUNT, identifierExpression);

    assertEquals(expected, aggregateExpressionConverter.convert(expression, requestContext));
  }
}
