package org.hypertrace.entity.query.service.converter.response.getter;

import static com.google.inject.name.Names.named;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.util.List;

public class GetterModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(ValueGetter.class).annotatedWith(named("array")).to(ArrayGetter.class);
    bind(ValueGetter.class).annotatedWith(named("boolean")).to(BooleanGetter.class);
    bind(ValueGetter.class).annotatedWith(named("bytes")).to(BytesGetter.class);
    bind(ValueGetter.class).annotatedWith(named("double")).to(DoubleGetter.class);
    bind(ValueGetter.class).annotatedWith(named("float")).to(FloatGetter.class);
    bind(ValueGetter.class).annotatedWith(named("int")).to(IntegerGetter.class);
    bind(ValueGetter.class).annotatedWith(named("long")).to(LongGetter.class);
    bind(ValueGetter.class).annotatedWith(named("string")).to(StringGetter.class);

    bind(ValueGetter.class).annotatedWith(named("direct_value")).to(DirectValueGetter.class);
    bind(ValueGetter.class).annotatedWith(named("list_value")).to(ListValueGetter.class);
    bind(ValueGetter.class).annotatedWith(named("map_value")).to(MapValueGetter.class);
    bind(ValueGetter.class).annotatedWith(named("nested_value")).to(NestedValueGetter.class);
  }

  @Provides
  @Singleton
  @Inject
  @Named("primitive_getters")
  List<ValueGetter> getPrimitiveValueGetters(
      @Named("boolean") final ValueGetter booleanGetter,
      @Named("bytes") final ValueGetter bytesGetter,
      @Named("double") final ValueGetter doubleGetter,
      @Named("float") final ValueGetter floatGetter,
      @Named("int") final ValueGetter intGetter,
      @Named("long") final ValueGetter longGetter,
      @Named("string") final ValueGetter stringGetter,
      @Named("array") final ValueGetter arrayGetter) {
    return List.of(
        booleanGetter,
        bytesGetter,
        intGetter,
        longGetter,
        floatGetter,
        doubleGetter,
        stringGetter,
        arrayGetter);
  }

  @Singleton
  @Provides
  @Inject
  @Named("root_getters")
  List<ValueGetter> getRootValueGetters(
      @Named("direct_value") final ValueGetter directValueGetter,
      @Named("nested_value") final ValueGetter nestedValueGetter,
      @Named("list_value") final ValueGetter listValueGetter,
      @Named("map_value") final ValueGetter mapValueGetter) {
    return List.of(nestedValueGetter, listValueGetter, mapValueGetter, directValueGetter);
  }
}
