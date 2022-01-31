package org.hypertrace.entity.query.service.converter.identifier;

import static org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter.getSubDocPathById;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class IdentifierExpressionConverter
    implements Converter<ColumnIdentifier, IdentifierExpression> {
  private final EntityAttributeMapping attributeMapping;

  @Override
  public IdentifierExpression convert(
      final ColumnIdentifier identifier, final RequestContext requestContext)
      throws ConversionException {
    final String subDocPath =
        getSubDocPathById(attributeMapping, identifier.getColumnName(), requestContext);
    return IdentifierExpression.of(subDocPath);
  }
}
