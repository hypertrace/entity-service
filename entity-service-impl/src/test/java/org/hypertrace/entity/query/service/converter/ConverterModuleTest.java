package org.hypertrace.entity.query.service.converter;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.inject.Guice;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.entity.query.service.EntityAttributeMapping;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ConverterModuleTest {
  @Mock private EntityAttributeMapping attributeMapping;

  @Test
  void testGetQueryConverter() {
    Converter<EntityQueryRequest, Query> queryConverter =
        Guice.createInjector(new ConverterModule(attributeMapping))
            .getInstance(Key.get(new TypeLiteral<Converter<EntityQueryRequest, Query>>() {}));
    assertNotNull(queryConverter);
  }
}
