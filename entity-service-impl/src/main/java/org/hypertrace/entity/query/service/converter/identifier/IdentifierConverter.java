package org.hypertrace.entity.query.service.converter.identifier;

import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.common.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;

public abstract class IdentifierConverter
    implements Converter<IdentifierConversionMetadata, String> {
  public static String getSubDocPathById(
      final EntityAttributeMapping attributeMapping,
      final String id,
      final RequestContext requestContext)
      throws ConversionException {
    final Optional<String> maybeSubDocPath =
        attributeMapping.getDocStorePathByAttributeId(requestContext, id);

    if (maybeSubDocPath.isEmpty()) {
      throw new ConversionException(String.format("Unable to get sub-doc-path for %s", id));
    }

    return maybeSubDocPath.get();
  }
}
