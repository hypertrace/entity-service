package org.hypertrace.entity.query.service.converter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.entity.query.service.v1.OrderByExpression;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class SortConverter implements Converter<List<OrderByExpression>, Sort> {

  @Override
  public Sort convert(List<OrderByExpression> proto) throws ConversionException {
    return null;
  }
}
