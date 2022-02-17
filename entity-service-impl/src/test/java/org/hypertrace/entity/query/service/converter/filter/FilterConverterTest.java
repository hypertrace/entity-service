package org.hypertrace.entity.query.service.converter.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.Operator;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class FilterConverterTest {
  @Mock private ExtraFiltersApplier extraFiltersApplier;
  @Mock private FilterTypeExpression allFilters;
  @Mock private LogicalExpressionConverter logicalFilterConverter;
  @Mock private RelationalExpressionConverter relationalExpressionConverter;

  private final RelationalExpression relationalExpression =
      RelationalExpression.of(
          IdentifierExpression.of("planet"), RelationalOperator.EQ, ConstantExpression.of("Mars"));
  private final Filter filter = Filter.newBuilder().setOperator(Operator.EQ).build();
  private final EntityQueryRequest request =
      EntityQueryRequest.newBuilder().setFilter(filter).build();
  private final EntityQueryRequest emptyRequest = EntityQueryRequest.getDefaultInstance();
  private final RequestContext requestContext = RequestContext.forTenantId("some-tenant-from-Mars");

  private FilterConverter filterConverter;

  @BeforeEach
  void setup() throws ConversionException {
    FilterConverterFactory filterConverterFactory =
        new FilterConverterFactoryImpl(relationalExpressionConverter, logicalFilterConverter);
    filterConverter = new FilterConverter(filterConverterFactory, extraFiltersApplier);

    when(extraFiltersApplier.getExtraFilters(emptyRequest, requestContext)).thenReturn(allFilters);
    when(relationalExpressionConverter.convert(filter, requestContext))
        .thenReturn(relationalExpression);
    when(extraFiltersApplier.addExtraFilters(relationalExpression, request, requestContext))
        .thenReturn(allFilters);
  }

  @Test
  void testConvert() throws ConversionException {
    assertEquals(allFilters, filterConverter.convert(request, requestContext).getExpression());
  }

  @Test
  void testConvertNoFilters() throws ConversionException {
    assertEquals(allFilters, filterConverter.convert(emptyRequest, requestContext).getExpression());
  }
}
