package org.hypertrace.entity.query.service.converter.identifier;

import static org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter.isPartOfAttributeMap;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.v1.ValueType;

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

    if (isFilteringFieldPrimitive(requestContext, columnId) || isFilterValueArray(valueType)) {
      return primitiveSuffixAddingIdentifierConverter;
    }

    if (isFilteringFieldMap(requestContext, columnId)) {
      return mapSuffixAddingIdentifierConverter;
    }

    throw new ConversionException(
        String.format(
            "Couldn't determine IdentifierConverter for column: %s, type: %s",
            columnId, valueType));
  }

  private boolean isFilteringFieldArray(
      final RequestContext requestContext, final String columnId) {
    final Optional<AttributeKind> attributeKind =
        attributeMapping.getAttributeKind(requestContext, columnId);
    return attributeKind.isPresent() && attributeMapping.isArray(attributeKind.get());
  }

  private boolean isFilterValueArray(final ValueType valueType) {
    return valueHelper.isArray(valueType);
  }

  private boolean isFilteringFieldMap(final RequestContext context, final String columnId) {
    final Optional<AttributeKind> attributeKind =
        attributeMapping.getAttributeKind(context, columnId);
    return attributeKind.isPresent() && attributeMapping.isMap(attributeKind.get());
  }

  private boolean isFilteringFieldPrimitive(final RequestContext context, final String columnId) {
    final Optional<AttributeKind> attributeKind =
        attributeMapping.getAttributeKind(context, columnId);
    return attributeKind.isPresent() && attributeMapping.isPrimitive(attributeKind.get());
  }
}
