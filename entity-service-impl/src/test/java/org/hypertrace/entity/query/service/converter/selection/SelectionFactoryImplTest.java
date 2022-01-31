package org.hypertrace.entity.query.service.converter.selection;

import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.AGGREGATION;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class SelectionFactoryImplTest {
  @Mock
  private Converter<org.hypertrace.entity.query.service.v1.AggregateExpression, AggregateExpression>
      aggregateExpressionConverter;

  @Mock private Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;

  @Mock
  private AliasProvider<org.hypertrace.entity.query.service.v1.AggregateExpression>
      aggregateAliasProvider;

  @Mock private AliasProvider<ColumnIdentifier> identifierAliasProvider;

  private SelectionFactory selectionFactory;

  @BeforeEach
  void setup() {
    selectionFactory =
        new SelectionFactoryImpl(
            aggregateExpressionConverter,
            identifierExpressionConverter,
            aggregateAliasProvider,
            identifierAliasProvider);
  }

  @ParameterizedTest
  @EnumSource(
      value = ValueCase.class,
      names = {"COLUMNIDENTIFIER", "AGGREGATION"},
      mode = Mode.EXCLUDE)
  void testThrowsException(final ValueCase valueCase) {
    assertThrows(ConversionException.class, () -> selectionFactory.getConverter(valueCase));
    assertThrows(ConversionException.class, () -> selectionFactory.getAliasProvider(valueCase));
  }

  @Test
  void testGetForAggregation() throws ConversionException {
    assertEquals(aggregateExpressionConverter, selectionFactory.getConverter(AGGREGATION));
    assertEquals(aggregateAliasProvider, selectionFactory.getAliasProvider(AGGREGATION));
  }

  @Test
  void testGetForColumnIdentifier() throws ConversionException {
    assertEquals(identifierExpressionConverter, selectionFactory.getConverter(COLUMNIDENTIFIER));
    assertEquals(identifierAliasProvider, selectionFactory.getAliasProvider(COLUMNIDENTIFIER));
  }
}
