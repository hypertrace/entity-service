package org.hypertrace.entity.query.service.converter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.query.service.v1.LiteralConstant;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class ConstantExpressionConverter implements Converter<LiteralConstant, ConstantExpression> {
  private final ValueHelper valueHelper;

  @Override
  public ConstantExpression convert(
      final LiteralConstant literalConstant, final RequestContext requestContext)
      throws ConversionException {
    return valueHelper.convertToConstantExpression(literalConstant.getValue());
  }
}
