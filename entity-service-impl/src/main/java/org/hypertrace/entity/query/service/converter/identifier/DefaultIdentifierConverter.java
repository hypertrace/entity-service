package org.hypertrace.entity.query.service.converter.identifier;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.converter.ConversionException;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class DefaultIdentifierConverter extends IdentifierConverter {

  @Override
  public String convert(
      final IdentifierConversionMetadata metadata, final RequestContext requestContext)
      throws ConversionException {
    return metadata.getSubDocPath();
  }
}
