package org.hypertrace.entity.query.service.converter;
/**
 * A generic converter interface to convert from proto-type (message/enum) to document-store type
 *
 * @param <T> The proto class
 * @param <U> The document store class
 */
public interface Converter<T, U> {
  U convert(T proto) throws ConversionException;
}
