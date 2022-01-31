package org.hypertrace.entity.query.service.converter;

import org.hypertrace.core.grpcutils.context.RequestContext;

/**
 * A generic converter interface to convert from proto-type (message/enum) to document-store type
 *
 * @param <T> The proto class
 * @param <U> The document store class
 */
public interface Converter<T, U> {
  U convert(final T proto, final RequestContext requestContext) throws ConversionException;
}
