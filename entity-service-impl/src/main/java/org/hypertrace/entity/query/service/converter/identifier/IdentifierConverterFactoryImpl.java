package org.hypertrace.entity.query.service.converter.identifier;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.EntityAttributeMapping;
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

    if (valueHelper.isPrimitive(valueType)) {
      return primitiveSuffixAddingIdentifierConverter;
    }

    if (valueHelper.isMap(valueType)) {
      return mapSuffixAddingIdentifierConverter;
    }

    if (valueHelper.isArray(valueType)) {
      if (attributeMapping.isMultiValued(requestContext, columnId)) {
        return arraySuffixAddingIdentifierConverter;
      } else {
        return primitiveSuffixAddingIdentifierConverter;
      }
    }

    throw new ConversionException(
        String.format(
            "Couldn't determine IdentifierConverter for column: %s, type: %s",
            columnId, valueType));
  }

  private static boolean isPartOfAttributeMap(final String subDocPath) {
    return subDocPath.startsWith(EntityConstants.ATTRIBUTES_MAP_PATH);
  }
}
