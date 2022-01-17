package org.hypertrace.entity.query.service.converter;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.EntityAttributeMapping;

@AllArgsConstructor
public class ConverterModule extends AbstractModule {
  private final EntityAttributeMapping attributeMapping;
  private final RequestContext requestContext;

  @Override
  protected void configure() {

  }

  @Provides
  EntityAttributeMapping getEntityAttributeMapping() {
    return attributeMapping;
  }

  @Provides
  RequestContext getRequestContext() {
    return requestContext;
  }
}
