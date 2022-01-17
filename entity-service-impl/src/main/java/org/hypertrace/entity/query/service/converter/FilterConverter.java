package org.hypertrace.entity.query.service.converter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.query.Filter;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class FilterConverter implements Converter<org.hypertrace.entity.query.service.v1.Filter, Filter> {

  @Override
  public Filter convert(final org.hypertrace.entity.query.service.v1.Filter filter)
      throws ConversionException {
    // TODO:
    return null;
  }
}
