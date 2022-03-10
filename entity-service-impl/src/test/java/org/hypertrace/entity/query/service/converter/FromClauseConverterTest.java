package org.hypertrace.entity.query.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.UnnestExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.accessor.ExpressionOneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FromClauseConverterTest {

  @Mock private Converter<ColumnIdentifier, IdentifierExpression> mockIdentifierExpressionConverter;
  @Mock private EntityAttributeMapping mockEntityAttributeMapping;

  private Converter<List<Expression>, List<FromTypeExpression>> fromClauseConverter;

  @BeforeEach
  void setUp() {
    final ExpressionOneOfAccessor expressionOneOfAccessor = new ExpressionOneOfAccessor();
    fromClauseConverter =
        new FromClauseConverter(
            expressionOneOfAccessor, mockIdentifierExpressionConverter, mockEntityAttributeMapping);
  }

  @Test
  void testConvert1() throws Exception {
    final ColumnIdentifier col1 = ColumnIdentifier.newBuilder().setColumnName("col1").build();

    final ColumnIdentifier col2 = ColumnIdentifier.newBuilder().setColumnName("col2").build();

    final List<Expression> expressions =
        List.of(
            Expression.newBuilder().setColumnIdentifier(col1).build(),
            Expression.newBuilder().setColumnIdentifier(col2).build());

    final RequestContext requestContext = new RequestContext();
    when(mockEntityAttributeMapping.isMultiValued(eq(requestContext), eq("col1")))
        .thenReturn(false);
    when(mockEntityAttributeMapping.isMultiValued(eq(requestContext), eq("col2"))).thenReturn(true);
    when(mockIdentifierExpressionConverter.convert(col2, requestContext))
        .thenReturn(IdentifierExpression.of("name"));

    final List<FromTypeExpression> expected =
        List.of(UnnestExpression.of(IdentifierExpression.of("name"), false));
    final List<FromTypeExpression> actual =
        fromClauseConverter.convert(expressions, requestContext);

    assertEquals(expected, actual);
  }

  @Test
  void testConvert1_ConverterThrowsConversionException() {
    final ColumnIdentifier col1 = ColumnIdentifier.newBuilder().setColumnName("col1").build();
    final ColumnIdentifier col2 = ColumnIdentifier.newBuilder().setColumnName("col2").build();

    final List<Expression> expressions =
        List.of(
            Expression.newBuilder().setColumnIdentifier(col1).build(),
            Expression.newBuilder()
                .setFunction(
                    Function.newBuilder()
                        .addArguments(Expression.newBuilder().setColumnIdentifier(col2)))
                .build());

    final RequestContext requestContext = new RequestContext();

    assertThrows(
        ConversionException.class, () -> fromClauseConverter.convert(expressions, requestContext));
  }
}
