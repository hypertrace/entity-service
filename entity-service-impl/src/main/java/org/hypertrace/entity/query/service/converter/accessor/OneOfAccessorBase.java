package org.hypertrace.entity.query.service.converter.accessor;

import static com.google.common.base.Suppliers.memoize;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.hypertrace.entity.query.service.converter.ConversionException;

public abstract class OneOfAccessorBase<T, U extends Enum<U>> implements OneOfAccessor<T, U> {
  private final Supplier<Map<U, Function<T, ?>>> ACCESSOR_MAP = memoize(this::populate);

  @Override
  @SuppressWarnings("unchecked")
  public final <V> V access(final T proto, final U enumValue) throws ConversionException {
    final Function<T, ?> accessor = ACCESSOR_MAP.get().get(enumValue);

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

  @Override
  public <V> V accessListElement(final T proto, final U valueCaseEnum, final int index)
      throws ConversionException {
    final List<V> list = access(proto, valueCaseEnum);
    return list.get(index);
  }

  @Override
  public <K, V> V accessMapValue(final T proto, final U valueCaseEnum, final K key)
      throws ConversionException {
    final Map<K, V> map = access(proto, valueCaseEnum);
    return map.get(key);
  }

  protected abstract Map<U, Function<T, ?>> populate();
}
