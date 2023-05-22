package org.hypertrace.entity.query.service.converter.filter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class FilteringExpressionConverterFactoryImpl
    implements FilteringExpressionConverterFactory {
  private static final Set<Operator> CONTAINMENT_OPERATORS = Set.of(Operator.IN, Operator.NOT_IN);
  private NullFilteringExpressionConverter nullFilteringExpressionConverter;
  private PrimitiveFilteringExpressionConverter primitiveFilteringExpressionConverter;
  private ArrayFilteringExpressionConverter arrayFilteringExpressionConverter;
  private MapFilteringExpressionConverter mapFilteringExpressionConverter;
  private ValueHelper valueHelper;
  private ContainsFilteringExpressionConverter containsFilteringExpressionConverter;
  private EntityAttributeMapping entityAttributeMapping;

  @Override
  public FilteringExpressionConverter getConverter(
      final ColumnIdentifier identifier,
      final Operator operator,
      final Value value,
      final RequestContext context)
      throws ConversionException {
    ValueType valueType = value.getValueType();

    // should always be first
    if (valueHelper.isNull(value)) {
      return nullFilteringExpressionConverter;
    }

    if (CONTAINMENT_OPERATORS.contains(operator)) {
      if (entityAttributeMapping.isArray(context, identifier.getColumnName())) {
        return containsFilteringExpressionConverter;
      }
      return primitiveFilteringExpressionConverter;
    }

    if (valueHelper.isPrimitive(valueType)) {
      return primitiveFilteringExpressionConverter;
    }

    if (valueHelper.isArray(valueType)) {
      return arrayFilteringExpressionConverter;
    }

    if (valueHelper.isMap(valueType)) {
      return mapFilteringExpressionConverter;
    }

    throw new ConversionException(String.format("Unknown value type: %s", valueType));
  }
}
