package org.hypertrace.entity.query.service.converter.identifier;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ValueType;

public interface IdentifierConverterFactory {
  IdentifierConverter getIdentifierConverter(
      final String columnId,
      final String subDocPath,
      final ValueType valueType,
      final RequestContext requestContext)
      throws ConversionException;
}
