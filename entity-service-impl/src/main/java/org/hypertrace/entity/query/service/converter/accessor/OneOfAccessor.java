package org.hypertrace.entity.query.service.converter.accessor;

import java.util.Set;
import org.hypertrace.entity.query.service.converter.ConversionException;

public interface OneOfAccessor<T, U extends Enum<U>> {
  <V> V access(final T proto, final U valueCaseEnum) throws ConversionException;

  <V> V access(final T proto, final U valueCaseEnum, final Set<U> allowedValueCases)
      throws ConversionException;
}
