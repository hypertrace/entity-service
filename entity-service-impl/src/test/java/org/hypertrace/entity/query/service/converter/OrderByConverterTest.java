package org.hypertrace.entity.query.service.converter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.quality.Strictness.LENIENT;

import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.SortOrder;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.accessor.ExpressionOneOfAccessor;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.converter.identifier.FunctionAliasProvider;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierAliasProvider;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Function;
import org.hypertrace.entity.query.service.v1.OrderByExpression;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.EnumSource.Mode;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class OrderByConverterTest {
  @Mock private RequestContext requestContext;

  private Converter<List<OrderByExpression>, Sort> orderByConverter;

  private final AliasProvider<ColumnIdentifier> aliasProvider = new IdentifierAliasProvider();
  private final AliasProvider<Function> functionAliasProvider = new FunctionAliasProvider();
  private final ColumnIdentifier columnIdentifier =
      ColumnIdentifier.newBuilder().setColumnName("Planet_Mars").build();
  private final IdentifierExpression identifierExpression = IdentifierExpression.of("Planet_Mars");
  private final OrderByExpression orderByExpression =
      OrderByExpression.newBuilder()
          .setExpression(Expression.newBuilder().setColumnIdentifier(columnIdentifier))
          .setOrder(org.hypertrace.entity.query.service.v1.SortOrder.ASC)
          .build();

  @BeforeEach
  void setup() {
    OneOfAccessor<Expression, ValueCase> expressionAccessor = new ExpressionOneOfAccessor();
    orderByConverter =
        new OrderByConverter(expressionAccessor, aliasProvider, functionAliasProvider);
  }

  @Test
  void testConvert() throws ConversionException {
    Sort expected =
        Sort.builder().sortingSpec(SortingSpec.of(identifierExpression, SortOrder.ASC)).build();
    assertEquals(expected, orderByConverter.convert(List.of(orderByExpression), requestContext));
  }

  @ParameterizedTest
  @EnumSource(
      value = org.hypertrace.entity.query.service.v1.SortOrder.class,
      mode = Mode.EXCLUDE,
      names = "UNRECOGNIZED")
  void testConvertCoverage(final org.hypertrace.entity.query.service.v1.SortOrder sortOrder)
      throws ConversionException {
    OrderByExpression orderByExpression =
        OrderByExpression.newBuilder()
            .setExpression(Expression.newBuilder().setColumnIdentifier(columnIdentifier))
            .setOrder(sortOrder)
            .build();

    assertNotNull(orderByConverter.convert(List.of(orderByExpression), requestContext));
  }
}
