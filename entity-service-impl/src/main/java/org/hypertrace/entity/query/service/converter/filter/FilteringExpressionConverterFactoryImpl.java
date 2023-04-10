package org.hypertrace.entity.query.service.converter.filter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
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
      final String columnName, final Value value, final RequestContext context)
      throws ConversionException {
    ValueType valueType = value.getValueType();

    // should always be first
    if (valueHelper.isNull(value)) {
      return nullFilteringExpressionConverter;
    }

    final AttributeKind attributeKind =
        entityAttributeMapping
            .getAttributeKind(context, columnName)
            .orElseThrow(
                () ->
                    new ConversionException(
                        String.format("Cannot find attribute kind for %s", columnName)));

    if (entityAttributeMapping.isPrimitive(attributeKind)) {
      return primitiveFilteringExpressionConverter;
    }

    if (entityAttributeMapping.isArray(attributeKind)) {
      return arrayFilteringExpressionConverter;
    }

    if (entityAttributeMapping.isMap(attributeKind)) {
      return mapFilteringExpressionConverter;
    }

    throw new ConversionException(String.format("Unknown value type: %s", valueType));
  }
}
