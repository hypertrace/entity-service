package org.hypertrace.entity.query.service.converter.accessor;

import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.FUNCTION;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.LITERAL;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.ORDERBY;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.VALUE_NOT_SET;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.inject.Guice;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import java.util.Set;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Function;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.OrderByExpression;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class ExpressionOneOfAccessorTest {
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor =
      Guice.createInjector(new AccessorModule())
          .getInstance(Key.get(new TypeLiteral<OneOfAccessor<Expression, ValueCase>>() {}));

  @ParameterizedTest
  @EnumSource(ValueCase.class)
  void testValueCaseCoverage(final ValueCase valueCase) throws ConversionException {
    Expression expression = Expression.newBuilder().build();

    if (valueCase != VALUE_NOT_SET) {
      assertNotNull(expressionAccessor.access(expression, valueCase));
    }
  }

  @Test
  void testAccess() throws ConversionException {
    Expression expression = Expression.newBuilder().build();
    assertEquals(
        ColumnIdentifier.getDefaultInstance(),
        expressionAccessor.access(expression, COLUMNIDENTIFIER));
    assertEquals(
        LiteralConstant.getDefaultInstance(), expressionAccessor.access(expression, LITERAL));
    assertEquals(Function.getDefaultInstance(), expressionAccessor.access(expression, FUNCTION));
    assertEquals(
        OrderByExpression.getDefaultInstance(), expressionAccessor.access(expression, ORDERBY));
  }

  @Test
  void testAccessWithAllowedValueCases() throws ConversionException {
    Expression expression = Expression.newBuilder().build();
    assertEquals(
        ColumnIdentifier.getDefaultInstance(),
        expressionAccessor.access(expression, COLUMNIDENTIFIER, Set.of(COLUMNIDENTIFIER, LITERAL)));
    assertEquals(
        LiteralConstant.getDefaultInstance(),
        expressionAccessor.access(expression, LITERAL, Set.of(COLUMNIDENTIFIER, LITERAL)));
    assertThrows(
        ConversionException.class,
        () -> expressionAccessor.access(expression, FUNCTION, Set.of(COLUMNIDENTIFIER, LITERAL)));
    assertThrows(
        ConversionException.class,
        () -> expressionAccessor.access(expression, ORDERBY, Set.of(COLUMNIDENTIFIER, LITERAL)));
  }
}
