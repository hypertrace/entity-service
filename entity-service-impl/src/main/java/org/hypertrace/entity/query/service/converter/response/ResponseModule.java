package org.hypertrace.entity.query.service.converter.response;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.hypertrace.entity.query.service.converter.response.getter.GetterModule;

public class ResponseModule extends AbstractModule {

  @Override
  protected void configure() {
    bind(DocumentConverter.class).to(DocumentConverterImpl.class);
    install(new GetterModule());
  }

  @Singleton
  @Provides
  ObjectMapper provideObjectMapper() {
    return new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }
}
