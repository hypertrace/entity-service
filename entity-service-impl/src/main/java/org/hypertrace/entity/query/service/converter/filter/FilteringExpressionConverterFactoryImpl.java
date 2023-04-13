package org.hypertrace.entity.query.service.converter.filter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class FilteringExpressionConverterFactoryImpl
    implements FilteringExpressionConverterFactory {
  private EntityAttributeMapping entityAttributeMapping;
  private NullFilteringExpressionConverter nullFilteringExpressionConverter;
  private PrimitiveFilteringExpressionConverter primitiveFilteringExpressionConverter;
  private ArrayFilteringExpressionConverter arrayFilteringExpressionConverter;
  private MapFilteringExpressionConverter mapFilteringExpressionConverter;
  private ValueHelper valueHelper;

  @Override
  public FilteringExpressionConverter getConverter(
      final String columnName,
      final Value value,
      final Operator operator,
      final RequestContext context)
      throws ConversionException {
    ValueType valueType = value.getValueType();

    // should always be first
    if (valueHelper.isNull(value)) {
      return nullFilteringExpressionConverter;
    }

    //    try {
    //      Thread.sleep(200000);
    //    } catch (InterruptedException e) {
    //      throw new RuntimeException(e);
    //    }

    if (operator.equals(Operator.IN) || operator.equals(Operator.NOT_IN)) {
      return primitiveFilteringExpressionConverter;
    }

    if (entityAttributeMapping.isPrimitive(context, columnName)) {
      return primitiveFilteringExpressionConverter;
    }

    if (entityAttributeMapping.isArray(context, columnName)) {
      return arrayFilteringExpressionConverter;
    }

    if (entityAttributeMapping.isMap(context, columnName)) {
      return mapFilteringExpressionConverter;
    }

    throw new ConversionException(String.format("Unknown value type: %s", valueType));
  }
}
