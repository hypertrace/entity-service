package org.hypertrace.entity.query.service.converter.aggregation;

import static org.hypertrace.entity.query.service.v1.AggregationOperator.AGGREGATION_OPERATOR_DISTINCT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.accessor.ExpressionOneOfAccessor;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.AggregateExpression;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class AggregationAliasProviderTest {
  @Mock private AliasProvider<ColumnIdentifier> identifierAliasProvider;

  private AliasProvider<AggregateExpression> aggregationAliasProvider;
  private AggregateExpression.Builder aggregateExpressionBuilder;

  private ColumnIdentifier.Builder columnIdentifierBuilder;

  @BeforeEach
  void setup() throws ConversionException {
    OneOfAccessor<Expression, ValueCase> expressionAccessor = new ExpressionOneOfAccessor();
    columnIdentifierBuilder = ColumnIdentifier.newBuilder().setColumnName("Welcome_Mars");

    aggregationAliasProvider =
        new AggregationAliasProvider(identifierAliasProvider, expressionAccessor);
    aggregateExpressionBuilder =
        AggregateExpression.newBuilder()
            .setExpression(Expression.newBuilder().setColumnIdentifier(columnIdentifierBuilder));

    when(identifierAliasProvider.getAlias(columnIdentifierBuilder.build()))
        .thenReturn("Welcome_Mars");
  }

  @Test
  void testGetAlias() throws ConversionException {
    AggregateExpression expression =
        aggregateExpressionBuilder.setOperator(AGGREGATION_OPERATOR_DISTINCT).build();
    assertEquals(
        "AGGREGATION_OPERATOR_DISTINCT_Welcome_Mars",
        aggregationAliasProvider.getAlias(expression));
  }

  @Test
  void testGetSetAlias() throws ConversionException {
    AggregateExpression expression =
        aggregateExpressionBuilder
            .setOperator(AGGREGATION_OPERATOR_DISTINCT)
            .setExpression(
                Expression.newBuilder()
                    .setColumnIdentifier(
                        columnIdentifierBuilder.setAlias("total_population_in_Mars")))
            .build();
    assertEquals("total_population_in_Mars", aggregationAliasProvider.getAlias(expression));
  }
}
