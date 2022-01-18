package org.hypertrace.entity.query.service.converter.identifier;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;

public class IdentifierModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<Converter<ColumnIdentifier, IdentifierExpression>>() {})
        .to(IdentifierExpressionConverter.class);
    bind(new TypeLiteral<AliasProvider<ColumnIdentifier>>() {}).to(IdentifierAliasProvider.class);
  }
}
