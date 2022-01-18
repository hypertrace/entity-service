package org.hypertrace.entity.query.service.converter.accessor;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.hypertrace.entity.query.service.v1.Expression;

public class AccessorModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<IOneOfAccessor<Expression, Expression.ValueCase>>() {})
        .to(ExpressionOneOfAccessor.class);
  }
}
