package org.hypertrace.entity.query.service.converter.filter;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;

public class FilterModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<Converter<EntityQueryRequest, Filter>>() {}).to(FilterConverter.class);
    bind(FilterConverterFactory.class).to(FilterConverterFactoryImpl.class);
    bind(ExtraFiltersApplier.class).to(ExtraFiltersApplierImpl.class);
    bind(FilteringExpressionConverterFactory.class)
        .to(FilteringExpressionConverterFactoryImpl.class);
  }
}
