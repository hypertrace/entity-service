package org.hypertrace.entity.query.service.converter;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;
import static org.hypertrace.entity.query.service.v1.SortOrder.ASC;
import static org.hypertrace.entity.query.service.v1.SortOrder.DESC;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.operators.SortingOrder;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.core.documentstore.query.SortingSpec;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.OrderByExpression;
import org.hypertrace.entity.query.service.v1.SortOrder;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class OrderByConverter implements Converter<List<OrderByExpression>, Sort> {
  private static final Supplier<Map<SortOrder, SortingOrder>> ORDER_MAP =
      memoize(OrderByConverter::getMapping);
  private final OneOfAccessor<Expression, ValueCase> expressionAccessor;
  private final AliasProvider<ColumnIdentifier> aliasProvider;

  @Override
  public Sort convert(final List<OrderByExpression> orders, final RequestContext requestContext)
      throws ConversionException {
    final List<SortingSpec> specs = new ArrayList<>();

    for (final OrderByExpression orderBy : orders) {
      final SortingSpec spec = convert(orderBy);
      specs.add(spec);
    }

    return Sort.builder().sortingSpecs(specs).build();
  }

  private SortingSpec convert(final OrderByExpression orderBy) throws ConversionException {
    final Expression innerExpression = orderBy.getExpression();

    final ColumnIdentifier identifier =
        expressionAccessor.access(
            innerExpression, innerExpression.getValueCase(), Set.of(COLUMNIDENTIFIER));
    final String alias = aliasProvider.getAlias(identifier);
    final IdentifierExpression identifierExpression = IdentifierExpression.of(alias);

    final SortingOrder order = ORDER_MAP.get().get(orderBy.getOrder());

    if (order == null) {
      throw new ConversionException(
          String.format("Cannot find sorting order %s", orderBy.getOrder()));
    }

    return SortingSpec.of(identifierExpression, order);
  }

  private static Map<SortOrder, SortingOrder> getMapping() {
    final Map<SortOrder, SortingOrder> map = new EnumMap<>(SortOrder.class);

    map.put(ASC, SortingOrder.ASC);
    map.put(DESC, SortingOrder.DESC);

    return unmodifiableMap(map);
  }
}
