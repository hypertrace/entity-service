package org.hypertrace.entity.query.service.converter.aggregation;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.query.Aggregation;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Function;

public class AggregationModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<Converter<Function, AggregateExpression>>() {})
        .to(AggregateExpressionConverter.class);
    bind(new TypeLiteral<AliasProvider<Function>>() {}).to(AggregationAliasProvider.class);
    bind(new TypeLiteral<Converter<List<Expression>, Aggregation>>() {}).to(GroupByConverter.class);
  }
}
