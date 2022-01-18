package org.hypertrace.entity.query.service.converter.filter;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.entity.query.service.converter.Converter;

public class FilterModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<Converter<org.hypertrace.entity.query.service.v1.Filter, Filter>>() {})
        .to(FilterConverter.class);
    bind(FilterConverterFactory.class).to(FilterConverterFactoryImpl.class);
    bind(new TypeLiteral<
            Converter<org.hypertrace.entity.query.service.v1.Filter, LogicalExpression>>() {})
        .to(LogicalExpressionConverter.class);
    bind(new TypeLiteral<
            Converter<org.hypertrace.entity.query.service.v1.Filter, RelationalExpression>>() {})
        .to(RelationalExpressionConverter.class);
  }
}
