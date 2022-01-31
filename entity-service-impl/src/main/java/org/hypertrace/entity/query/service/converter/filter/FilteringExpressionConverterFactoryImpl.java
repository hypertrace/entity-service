package org.hypertrace.entity.query.service.converter.filter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class FilteringExpressionConverterFactoryImpl
    implements FilteringExpressionConverterFactory {
  private PrimitiveFilteringExpressionConverter primitiveFilteringExpressionConverter;
  private ArrayFilteringExpressionConverter arrayFilteringExpressionConverter;
  private MapFilteringExpressionConverter mapFilteringExpressionConverter;
  private ValueHelper valueHelper;

  @Override
  public FilteringExpressionConverter getConverter(final ValueType valueType)
      throws ConversionException {
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
