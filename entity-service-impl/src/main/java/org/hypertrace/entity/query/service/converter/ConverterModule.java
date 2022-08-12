package org.hypertrace.entity.query.service.converter;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import java.util.List;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.type.FromTypeExpression;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.documentstore.query.Sort;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.accessor.AccessorModule;
import org.hypertrace.entity.query.service.converter.aggregation.AggregationModule;
import org.hypertrace.entity.query.service.converter.filter.FilterModule;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierModule;
import org.hypertrace.entity.query.service.converter.response.ResponseModule;
import org.hypertrace.entity.query.service.converter.selection.SelectionConverterModule;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.OrderByExpression;

@AllArgsConstructor
public class ConverterModule extends AbstractModule {
  private final EntityAttributeMapping attributeMapping;

  @Override
  protected void configure() {
    install(new AccessorModule());
    install(new AggregationModule());
    install(new FilterModule());
    install(new IdentifierModule());
    install(new SelectionConverterModule());
    install(new ResponseModule());

    bind(new TypeLiteral<Converter<LiteralConstant, ConstantExpression>>() {})
        .to(ConstantExpressionConverter.class);
    bind(new TypeLiteral<Converter<List<OrderByExpression>, Sort>>() {}).to(OrderByConverter.class);
    bind(new TypeLiteral<Converter<EntityQueryRequest, Query>>() {}).to(QueryConverter.class);
    bind(new TypeLiteral<Converter<List<Expression>, List<FromTypeExpression>>>() {})
        .to(FromClauseConverter.class);
  }

  @Provides
  EntityAttributeMapping getEntityAttributeMapping() {
    return attributeMapping;
  }
}
