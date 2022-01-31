package org.hypertrace.entity.query.service.converter.response;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.util.List;
import org.hypertrace.entity.query.service.converter.response.getter.DirectValueGetter;
import org.hypertrace.entity.query.service.converter.response.getter.GetterModule;
import org.hypertrace.entity.query.service.converter.response.getter.ListValueGetter;
import org.hypertrace.entity.query.service.converter.response.getter.MapValueGetter;
import org.hypertrace.entity.query.service.converter.response.getter.PrimitiveValueGetter;
import org.hypertrace.entity.query.service.converter.response.getter.ValueGetter;

public class ResponseModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(JsonNodeConverter.class).to(JsonNodeConverterImpl.class);
    install(new GetterModule());
  }

  @Singleton
  @Provides
  @Inject
  @Named("root_getters")
  List<ValueGetter> getRootGetters(
      final DirectValueGetter directValueGetter,
      final PrimitiveValueGetter primitiveValueGetter,
      final ListValueGetter listValueGetter,
      final MapValueGetter mapValueGetter) {
    return List.of(primitiveValueGetter, listValueGetter, mapValueGetter, directValueGetter);
  }
}
