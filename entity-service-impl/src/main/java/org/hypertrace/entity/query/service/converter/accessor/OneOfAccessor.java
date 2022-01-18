package org.hypertrace.entity.query.service.converter.accessor;

import com.google.common.collect.ImmutableMap;
import java.util.Set;
import java.util.function.Function;
import org.hypertrace.entity.query.service.converter.ConversionException;

public abstract class OneOfAccessor<T, U extends Enum<U>> implements IOneOfAccessor<T, U> {
  private final ImmutableMap<U, Function<T, ?>> map;
  private final ImmutableMap.Builder<U, Function<T, ?>> builder;

  public OneOfAccessor() {
    this.builder = ImmutableMap.builder();
    this.populate();
    this.map = builder.build();
  }

  protected final void put(final U enumValue, final Function<T, ?> function) {
    builder.put(enumValue, function);
  }

  @Override
  @SuppressWarnings("unchecked")
  public final <V> V access(final T proto, final U enumValue) throws ConversionException {
    final Function<T, ?> accessor = map.get(enumValue);

    if (accessor == null) {
      throw new ConversionException(
          String.format("No accessor found for type %s in value %s", enumValue, proto));
    }

    return (V) accessor.apply(proto);
  }

  @Override
  public <V> V access(final T proto, final U valueCaseEnum, final Set<U> allowedValueCases)
      throws ConversionException {
    if (!allowedValueCases.contains(valueCaseEnum)) {
      throw new ConversionException(String.format("%s in %s is unsupported", valueCaseEnum, proto));
    }

    return access(proto, valueCaseEnum);
  }

  protected abstract void populate();
}
