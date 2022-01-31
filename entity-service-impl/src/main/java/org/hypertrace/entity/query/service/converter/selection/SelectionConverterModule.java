package org.hypertrace.entity.query.service.converter.selection;

import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import java.util.List;
import org.hypertrace.core.documentstore.query.Selection;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.Expression;

public class SelectionConverterModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(new TypeLiteral<Converter<List<Expression>, Selection>>() {}).to(SelectionConverter.class);
    bind(SelectionFactory.class).to(SelectionFactoryImpl.class);
  }
}
