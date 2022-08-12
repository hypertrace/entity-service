package org.hypertrace.entity.query.service.converter.identifier;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.hypertrace.entity.service.constants.EntityConstants;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class IdentifierConverterFactoryImpl implements IdentifierConverterFactory {
  private final EntityAttributeMapping attributeMapping;
  private final DefaultIdentifierConverter defaultIdentifierConverter;
  private final PrimitiveSuffixAddingIdentifierConverter primitiveSuffixAddingIdentifierConverter;
  private final ArraySuffixAddingIdentifierConverter arraySuffixAddingIdentifierConverter;
  private final ArrayElementSuffixAddingIdentifierConverter
      arrayElementSuffixAddingIdentifierConverter;
  private final MapSuffixAddingIdentifierConverter mapSuffixAddingIdentifierConverter;
  private final ValueHelper valueHelper;

  @Override
  public IdentifierConverter getIdentifierConverter(
      final String columnId,
      final String subDocPath,
      final ValueType valueType,
      final RequestContext requestContext)
      throws ConversionException {
    final boolean isAttributeField = isPartOfAttributeMap(subDocPath);

    if (!isAttributeField) {
      return defaultIdentifierConverter;
    }

    if (isFilteringFieldArray(requestContext, columnId)) {
      // LHS is an array
      if (isFilterValueArray(valueType)) {
        // If the RHS is an array, do element by element comparison
        return arrayElementSuffixAddingIdentifierConverter;
      } else {
        // If the RHS is not an array, do direct comparison
        return arraySuffixAddingIdentifierConverter;
      }
    }

    if (isFilterValuePrimitive(valueType) || isFilterValueArray(valueType)) {
      return primitiveSuffixAddingIdentifierConverter;
    }

    if (isFilterValueMap(valueType)) {
      return mapSuffixAddingIdentifierConverter;
    }

    throw new ConversionException(
        String.format(
            "Couldn't determine IdentifierConverter for column: %s, type: %s",
            columnId, valueType));
  }

  private boolean isFilteringFieldArray(
      final RequestContext requestContext, final String columnId) {
    return attributeMapping.isMultiValued(requestContext, columnId);
  }

  private boolean isFilterValueArray(final ValueType valueType) {
    return valueHelper.isArray(valueType);
  }

  private boolean isFilterValueMap(final ValueType valueType) {
    return valueHelper.isMap(valueType);
  }

  private boolean isFilterValuePrimitive(final ValueType valueType) {
    return valueHelper.isPrimitive(valueType);
  }

  private static boolean isPartOfAttributeMap(final String subDocPath) {
    return subDocPath.startsWith(EntityConstants.ATTRIBUTES_MAP_PATH);
  }
}
