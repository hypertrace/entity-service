package org.hypertrace.entity.query.service.converter.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doReturn;
import static org.mockito.quality.Strictness.LENIENT;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.accessor.ExpressionOneOfAccessor;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class RelationalExpressionConverterTest {
  @Mock private Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;
  @Mock private Converter<LiteralConstant, ConstantExpression> constantExpressionConverter;

  private final ColumnIdentifier columnIdentifier =
      ColumnIdentifier.newBuilder().setColumnName("planet").build();
  private final LiteralConstant literalConstant =
      LiteralConstant.newBuilder().setValue(Value.newBuilder().setString("Pluto")).build();
  private final Filter filter =
      Filter.newBuilder()
          .setOperator(Operator.NOT_IN)
          .setLhs(Expression.newBuilder().setColumnIdentifier(columnIdentifier))
          .setRhs(Expression.newBuilder().setLiteral(literalConstant))
          .build();
  private final RequestContext requestContext = RequestContext.forTenantId("Martian");
  private final IdentifierExpression identifierExpression = IdentifierExpression.of("planet");
  private final ConstantExpression constantExpression = ConstantExpression.of("Pluto");

  private Converter<Filter, RelationalExpression> relationalExpressionConverter;

  @BeforeEach
  void setup() throws ConversionException {
    OneOfAccessor<Expression, ValueCase> expressionAccessor = new ExpressionOneOfAccessor();

    relationalExpressionConverter =
        new RelationalExpressionConverter(
            expressionAccessor, identifierExpressionConverter, constantExpressionConverter);

    doReturn(identifierExpression)
        .when(identifierExpressionConverter)
        .convert(columnIdentifier, requestContext);
    doReturn(constantExpression)
        .when(constantExpressionConverter)
        .convert(literalConstant, requestContext);
  }

  @Test
  void testConvert() throws ConversionException {
    RelationalExpression expected =
        RelationalExpression.of(
            identifierExpression, RelationalOperator.NOT_IN, constantExpression);
    assertEquals(expected, relationalExpressionConverter.convert(filter, requestContext));
  }

  @Test
  void testConvertInvalidOperator() {
    Filter filter = Filter.newBuilder().setOperator(Operator.AND).build();
    assertThrows(
        ConversionException.class,
        () -> relationalExpressionConverter.convert(filter, requestContext));
  }
}
