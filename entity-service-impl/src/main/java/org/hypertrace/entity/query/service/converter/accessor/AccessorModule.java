package org.hypertrace.entity.query.service.converter.accessor;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Expression.ValueCase;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

public class AccessorModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<OneOfAccessor<Expression, ValueCase>>() {})
        .to(ExpressionOneOfAccessor.class);
    bind(new TypeLiteral<OneOfAccessor<Value, ValueType>>() {}).to(ValueOneOfAccessor.class);
  }
}
