package org.hypertrace.entity.query.service.converter.filter;

import static org.hypertrace.entity.query.service.v1.Operator.AND;
import static org.hypertrace.entity.query.service.v1.Operator.EQ;
import static org.hypertrace.entity.query.service.v1.Operator.LIKE;
import static org.hypertrace.entity.query.service.v1.Operator.NEQ;
import static org.hypertrace.entity.query.service.v1.Operator.OR;
import static org.hypertrace.entity.query.service.v1.Operator.UNRECOGNIZED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.quality.Strictness.LENIENT;

import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.Operator;
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
class FilterConverterFactoryImplTest {
  @Mock private Converter<Filter, RelationalExpression> relationalExpressionConverter;
  @Mock private Converter<Filter, LogicalExpression> logicalExpressionConverter;

  private FilterConverterFactory filterConverterFactory;

  @BeforeEach
  void setup() {
    filterConverterFactory = new FilterConverterFactoryImpl(relationalExpressionConverter, logicalExpressionConverter);
  }

  @ParameterizedTest
  @EnumSource(Operator.class)
  void testOperatorCoverage(final Operator operator) throws ConversionException {
    if (operator == UNRECOGNIZED) {
      assertThrows(ConversionException.class, () -> filterConverterFactory.getFilterConverter(operator));
    } else {
      assertNotNull(filterConverterFactory.getFilterConverter(operator));
    }
  }

  @Test
  void testGetFilterConverter() throws ConversionException {
    assertEquals(logicalExpressionConverter, filterConverterFactory.getFilterConverter(AND));
    assertEquals(logicalExpressionConverter, filterConverterFactory.getFilterConverter(OR));
    assertEquals(relationalExpressionConverter, filterConverterFactory.getFilterConverter(EQ));
    assertEquals(relationalExpressionConverter, filterConverterFactory.getFilterConverter(NEQ));
    assertEquals(relationalExpressionConverter, filterConverterFactory.getFilterConverter(LIKE));
  }
}
