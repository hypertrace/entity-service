package org.hypertrace.entity.query.service.converter.response.getter;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.Value;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DirectValueGetter implements ValueGetter {
  @Named("base_getters")
  private final List<ValueGetter> primitiveValueGetters;

  @Override
  public boolean matches(final JsonNode jsonNode) {
    return primitiveValueGetters.stream().anyMatch(getter -> getter.matches(jsonNode));
  }

  @Override
  public Value getValue(final JsonNode jsonNode) throws ConversionException {
    for (final ValueGetter getter : primitiveValueGetters) {
      if (getter.matches(jsonNode)) {
        return getter.getValue(jsonNode);
      }
    }

    throw new ConversionException(String.format("Unable to convert node: %s", jsonNode));
  }
}
