package org.hypertrace.entity.query.service.converter.selection;

import static com.google.common.base.Suppliers.memoize;
import static java.util.Collections.unmodifiableMap;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.COLUMNIDENTIFIER;
import static org.hypertrace.entity.query.service.v1.Expression.ValueCase.FUNCTION;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.EnumMap;
import java.util.Map;
import java.util.function.Supplier;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.type.SelectTypeExpression;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Function;

@Singleton
public class SelectionFactoryImpl implements SelectionFactory {
  private final Converter<Function, AggregateExpression> aggregateExpressionConverter;
  private final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;

  private final AliasProvider<Function> aggregateAliasProvider;
  private final AliasProvider<ColumnIdentifier> identifierAliasProvider;

  private final Supplier<Map<ValueCase, Converter<?, ? extends SelectTypeExpression>>> converterMap;
  private final Supplier<Map<ValueCase, AliasProvider<?>>> aliasProviderMap;

  @Inject
  public SelectionFactoryImpl(
      final Converter<Function, AggregateExpression> aggregateExpressionConverter,
      final Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter,
      final AliasProvider<Function> aggregateAliasProvider,
      final AliasProvider<ColumnIdentifier> identifierAliasProvider) {
    this.aggregateExpressionConverter = aggregateExpressionConverter;
    this.identifierExpressionConverter = identifierExpressionConverter;

    this.aggregateAliasProvider = aggregateAliasProvider;
    this.identifierAliasProvider = identifierAliasProvider;

    this.converterMap = memoize(this::getConverterMap);
    this.aliasProviderMap = memoize(this::getAliasProviderMap);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> Converter<T, ? extends SelectTypeExpression> getConverter(final ValueCase valueCase)
      throws ConversionException {
    final Converter<?, ? extends SelectTypeExpression> converter =
        converterMap.get().get(valueCase);

    if (converter == null) {
      throw new ConversionException(
          String.format("Converter not found for %s", valueCase.toString().toLowerCase()));
    }

    return (Converter<T, ? extends SelectTypeExpression>) converter;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> AliasProvider<T> getAliasProvider(final ValueCase valueCase)
      throws ConversionException {
    final AliasProvider<?> aliasProvider = aliasProviderMap.get().get(valueCase);

    if (aliasProvider == null) {
      throw new ConversionException(
          String.format("Alias provider not found for %s", valueCase.toString().toLowerCase()));
    }

    return (AliasProvider<T>) aliasProvider;
  }

  private Map<ValueCase, Converter<?, ? extends SelectTypeExpression>> getConverterMap() {
    final Map<ValueCase, Converter<?, ? extends SelectTypeExpression>> map =
        new EnumMap<>(ValueCase.class);

    map.put(FUNCTION, aggregateExpressionConverter);
    map.put(COLUMNIDENTIFIER, identifierExpressionConverter);

    return unmodifiableMap(map);
  }

  private Map<ValueCase, AliasProvider<?>> getAliasProviderMap() {
    final Map<ValueCase, AliasProvider<?>> map = new EnumMap<>(ValueCase.class);

    map.put(FUNCTION, aggregateAliasProvider);
    map.put(COLUMNIDENTIFIER, identifierAliasProvider);

    return unmodifiableMap(map);
  }
}
