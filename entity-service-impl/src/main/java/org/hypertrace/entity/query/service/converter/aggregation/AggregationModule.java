package org.hypertrace.entity.query.service.converter.aggregation;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import java.util.List;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.query.Aggregation;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.GroupByExpression;

public class AggregationModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<
            Converter<
                org.hypertrace.entity.query.service.v1.AggregateExpression,
                AggregateExpression>>() {})
        .to(AggregateExpressionConverter.class);
    bind(new TypeLiteral<
            AliasProvider<org.hypertrace.entity.query.service.v1.AggregateExpression>>() {})
        .to(AggregationAliasProvider.class);
    bind(new TypeLiteral<Converter<List<GroupByExpression>, Aggregation>>() {})
        .to(GroupByConverter.class);
  }
}
