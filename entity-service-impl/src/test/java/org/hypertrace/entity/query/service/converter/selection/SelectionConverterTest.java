package org.hypertrace.entity.query.service.converter.selection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.core.documentstore.query.SelectionSpec;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.AliasProvider;
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
class SelectionConverterTest {
  private final RequestContext requestContext = RequestContext.forTenantId("first-man-from-Mars");
  private final ColumnIdentifier columnIdentifier =
      ColumnIdentifier.newBuilder().setColumnName("Hello_Mars").build();
  private final IdentifierExpression identifierExpression = IdentifierExpression.of("Hello_Mars");
  private final Expression expression =
      Expression.newBuilder().setColumnIdentifier(columnIdentifier).build();

  @Mock private SelectionFactory selectionFactory;
  @Mock private Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;
  @Mock private AliasProvider<ColumnIdentifier> identifierAliasProvider;

  private SelectionConverter selectionConverter;

  @BeforeEach
  void setup() throws ConversionException {
    OneOfAccessor<Expression, ValueCase> expressionAccessor = new ExpressionOneOfAccessor();

    selectionConverter = new SelectionConverter(selectionFactory, expressionAccessor);

    doReturn(identifierExpressionConverter)
        .when(selectionFactory)
        .getConverter(any(ValueCase.class));
    doReturn(identifierAliasProvider).when(selectionFactory).getAliasProvider(any(ValueCase.class));

    when(identifierExpressionConverter.convert(columnIdentifier, requestContext))
        .thenReturn(identifierExpression);
    when(identifierAliasProvider.getAlias(columnIdentifier)).thenReturn("Hello_Mars");
  }

  @Test
  void testConvert() throws ConversionException {
    Selection expected =
        Selection.builder()
            .selectionSpec(SelectionSpec.of(IdentifierExpression.of("Hello_Mars"), "Hello_Mars"))
            .build();
    assertEquals(expected, selectionConverter.convert(List.of(expression), requestContext));
  }
}
