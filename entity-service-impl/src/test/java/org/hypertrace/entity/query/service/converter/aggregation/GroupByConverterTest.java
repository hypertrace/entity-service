package org.hypertrace.entity.query.service.converter.aggregation;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.query.Aggregation;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class GroupByConverterTest {
  @Mock private Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;
  @Mock private RequestContext requestContext;

  private Converter<List<Expression>, Aggregation> groupByConverter;

  private final ColumnIdentifier columnIdentifier =
      ColumnIdentifier.newBuilder().setColumnName("Planet_Mars").build();
  private final IdentifierExpression identifierExpression = IdentifierExpression.of("Planet_Mars");
  private final Expression expression =
      Expression.newBuilder().setColumnIdentifier(columnIdentifier).build();

  @BeforeEach
  void setup() throws ConversionException {
    OneOfAccessor<Expression, ValueCase> expressionAccessor = new ExpressionOneOfAccessor();
    groupByConverter = new GroupByConverter(expressionAccessor, identifierExpressionConverter);

    when(identifierExpressionConverter.convert(columnIdentifier, requestContext))
        .thenReturn(identifierExpression);
  }

  @Test
  void testConvert() throws ConversionException {
    Aggregation expected = Aggregation.builder().expression(identifierExpression).build();
    assertEquals(expected, groupByConverter.convert(List.of(expression), requestContext));
  }
}
