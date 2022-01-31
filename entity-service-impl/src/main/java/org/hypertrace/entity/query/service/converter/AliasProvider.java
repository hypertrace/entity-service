package org.hypertrace.entity.query.service.converter;

public interface AliasProvider<T> {
  String getAlias(T expression) throws ConversionException;

  String ALIAS_SEPARATOR = "_";
}
