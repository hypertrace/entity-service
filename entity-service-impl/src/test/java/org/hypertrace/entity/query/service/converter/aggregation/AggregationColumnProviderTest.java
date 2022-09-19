package org.hypertrace.entity.query.service.converter.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class AggregationColumnProviderTest {

  private AggregationColumnProvider aggregationColumnProvider;

  @BeforeEach
  void setUp() {
    aggregationColumnProvider = new AggregationColumnProvider();
  }

  @Test
  void testGetColumnIdentifier() throws Exception {
    final Expression exp1 =
        Expression.newBuilder()
            .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName("col1"))
            .build();

    final Function function =
        Function.newBuilder().setFunctionName("DISTINCT_ARRAY").addArguments(exp1).build();

    final Expression result = aggregationColumnProvider.getAggregationColumn(function);

    assertEquals(exp1, result);
  }

  @Test
  void testGetColumnIdentifier_ThrowsConversionException() {
    final ColumnIdentifier col1 = ColumnIdentifier.newBuilder().setColumnName("col1").build();
    final ColumnIdentifier col2 = ColumnIdentifier.newBuilder().setColumnName("col2").build();

    final Function function =
        Function.newBuilder()
            .setFunctionName("DISTINCT_ARRAY")
            .addArguments(Expression.newBuilder().setColumnIdentifier(col1).build())
            .addArguments(Expression.newBuilder().setColumnIdentifier(col2).build())
            .build();

    assertThrows(
        ConversionException.class, () -> aggregationColumnProvider.getAggregationColumn(function));
  }
}
