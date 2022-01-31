package org.hypertrace.entity.query.service.converter.response.getter;

import com.google.inject.AbstractModule;
import com.google.inject.Inject;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.util.List;

public class GetterModule extends AbstractModule {
  @Provides
  @Singleton
  @Inject
  @Named("base_getters")
  List<ValueGetter> getValueGetters(final BooleanGetter booleanGetter, final BytesGetter bytesGetter, final DoubleGetter doubleGetter, final FloatGetter floatGetter, final IntegerGetter intGetter, final LongGetter longGetter, final StringGetter stringGetter) {
    return List.of(booleanGetter, bytesGetter, intGetter, longGetter, floatGetter, doubleGetter, stringGetter);
  }
}
