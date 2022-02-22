package org.hypertrace.entity.query.service.converter.aggregation;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.DISTINCT_COUNT;
import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.hypertrace.entity.query.service.v1.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
public class AggregationExpressionConverterTest {
  private Converter<Function, AggregateExpression> aggregateExpressionConverter;

  @Mock private Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;
  @Mock private RequestContext requestContext;

  private final ColumnIdentifier columnIdentifier =
      ColumnIdentifier.newBuilder().setColumnName("Hello_Mars").build();
  private final IdentifierExpression identifierExpression = IdentifierExpression.of("Hello_Mars");

  private Function.Builder aggregateExpressionBuilder;

  @BeforeEach
  void setup() throws ConversionException {
    OneOfAccessor<Expression, ValueCase> expressionAccessor = new ExpressionOneOfAccessor();
    aggregateExpressionConverter =
        new AggregateExpressionConverter(expressionAccessor, identifierExpressionConverter);
    aggregateExpressionBuilder =
        Function.newBuilder()
            .addArguments(Expression.newBuilder().setColumnIdentifier(columnIdentifier));

    when(identifierExpressionConverter.convert(columnIdentifier, requestContext))
        .thenReturn(identifierExpression);
  }

  @Test
  void testConvert() throws ConversionException {
    Function expression = aggregateExpressionBuilder.setFunctionName("DISTINCTCOUNT").build();
    AggregateExpression expected = AggregateExpression.of(DISTINCT_COUNT, identifierExpression);

    assertEquals(expected, aggregateExpressionConverter.convert(expression, requestContext));
  }
}
