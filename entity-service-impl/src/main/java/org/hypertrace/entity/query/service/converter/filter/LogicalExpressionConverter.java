package org.hypertrace.entity.query.service.converter.filter;

import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.entity.query.service.v1.Operator.AND;
import static org.hypertrace.entity.query.service.v1.Operator.OR;

import com.google.common.base.Suppliers;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.operators.LogicalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.Operator;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class LogicalExpressionConverter implements Converter<Filter, FilterTypeExpression> {
  private static final Supplier<Map<Operator, LogicalOperator>> LOGICAL_OPERATOR_MAP =
      Suppliers.memoize(LogicalExpressionConverter::getLogicalOperatorMap);
  private final FilterConverterFactory filterConverterFactory;

  @Override
  public FilterTypeExpression convert(final Filter filter, final RequestContext requestContext)
      throws ConversionException {
    final LogicalOperator operator = LOGICAL_OPERATOR_MAP.get().get(filter.getOperator());

    if (operator == null) {
      throw new ConversionException(
          String.format("No equivalent logical operator found for %s", filter.getOperator()));
    }

    if (filter.getChildFilterList().isEmpty()) {
      throw new ConversionException(
          String.format("No child filter found with operator: %s", operator));
    }

    final List<FilterTypeExpression> innerFilters = new ArrayList<>();
    for (final Filter innerFilter : filter.getChildFilterList()) {
      if (Filter.getDefaultInstance().equals(innerFilter)) {
        continue;
      }

      final FilterTypeExpression expression = convertInnerFilter(innerFilter, requestContext);
      innerFilters.add(expression);
    }

    switch (innerFilters.size()) {
      case 0:
        throw new ConversionException(String.format("All child filters are empty in: %s", filter));

      case 1:
        return innerFilters.get(0);

      default:
        return LogicalExpression.builder().operator(operator).operands(innerFilters).build();
    }
  }

  private FilterTypeExpression convertInnerFilter(
      final Filter filter, final RequestContext requestContext) throws ConversionException {
    final Converter<Filter, ? extends FilterTypeExpression> filterConverter =
        filterConverterFactory.getFilterConverter(filter.getOperator());
    return filterConverter.convert(filter, requestContext);
  }

  private static Map<Operator, LogicalOperator> getLogicalOperatorMap() {
    final Map<Operator, LogicalOperator> map = new EnumMap<>(Operator.class);

    map.put(AND, LogicalOperator.AND);
    map.put(OR, LogicalOperator.OR);

    return unmodifiableMap(map);
  }
}
