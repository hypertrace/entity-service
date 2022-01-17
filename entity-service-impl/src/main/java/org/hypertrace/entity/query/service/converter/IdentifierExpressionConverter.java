package org.hypertrace.entity.query.service.converter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.EntityAttributeMapping;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class IdentifierExpressionConverter implements Converter<ColumnIdentifier, IdentifierExpression> {
  private final EntityAttributeMapping attributeMapping;
  private final RequestContext requestContext;

  @Override
  public IdentifierExpression convert(final ColumnIdentifier identifier) throws ConversionException {
    final Optional<String> maybeSubDocPath = attributeMapping.getDocStorePathByAttributeId(requestContext, identifier.getColumnName());

    if (maybeSubDocPath.isEmpty()) {
      throw new ConversionException(String.format("Unable to get sub-doc-path for %s", identifier));
    }

    return IdentifierExpression.of(maybeSubDocPath.get());
  }
}
