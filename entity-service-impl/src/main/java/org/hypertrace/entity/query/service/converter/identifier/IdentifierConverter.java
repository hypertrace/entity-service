package org.hypertrace.entity.query.service.converter.identifier;

import static org.hypertrace.entity.service.constants.EntityConstants.ATTRIBUTES_MAP_PATH;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.Converter;

public abstract class IdentifierConverter
    implements Converter<IdentifierConversionMetadata, String> {
  public static String getSubDocPathById(
      final EntityAttributeMapping attributeMapping,
      final String id,
      final RequestContext requestContext) {
    // In the case of non-attribute id, the sub-doc-path is assumed to be the same as the id
    return attributeMapping.getDocStorePathByAttributeId(requestContext, id).orElse(id);
  }

  public static boolean isPartOfAttributeMap(final String subDocPath) {
    return subDocPath.startsWith(ATTRIBUTES_MAP_PATH);
  }
}
