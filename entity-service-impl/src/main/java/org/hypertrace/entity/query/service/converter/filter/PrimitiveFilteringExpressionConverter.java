package org.hypertrace.entity.query.service.converter.filter;

import static java.util.Map.entry;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BOOL;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BOOL_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BYTES;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_DOUBLE;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_DOUBLE_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_INT64;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_INT64_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING_MAP;
import static org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter.getSubDocPathById;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOL;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOLEAN_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_ARRAY;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_MAP;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConversionMetadata;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConverterFactory;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ValueType;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class PrimitiveFilteringExpressionConverter extends FilteringExpressionConverterBase {

  private static final Map<AttributeKind, ValueType> ATTRIBUTE_KIND_TO_VALUE_TYPEMAP =
      Map.ofEntries(
          entry(TYPE_STRING, STRING),
          entry(TYPE_INT64, LONG),
          entry(TYPE_DOUBLE, DOUBLE),
          entry(TYPE_BYTES, BYTES),
          entry(TYPE_BOOL, BOOL),
          entry(TYPE_STRING_ARRAY, STRING_ARRAY),
          entry(TYPE_INT64_ARRAY, LONG_ARRAY),
          entry(TYPE_DOUBLE_ARRAY, DOUBLE_ARRAY),
          entry(TYPE_BOOL_ARRAY, BOOLEAN_ARRAY),
          entry(TYPE_STRING_MAP, STRING_MAP));
  private final EntityAttributeMapping entityAttributeMapping;
  private final IdentifierConverterFactory identifierConverterFactory;
  private final Converter<LiteralConstant, ConstantExpression> constantExpressionConverter;

  @Override
  public FilterTypeExpression convert(
      final ColumnIdentifier columnIdentifier,
      final Operator operator,
      final LiteralConstant constant,
      final RequestContext requestContext)
      throws ConversionException {
    final String id = columnIdentifier.getColumnName();
    final String subDocPath = getSubDocPathById(entityAttributeMapping, id, requestContext);
    final Optional<AttributeKind> attributeKind =
        entityAttributeMapping.getAttributeKind(requestContext, columnIdentifier.getColumnName());
    final ValueType valueType =
        attributeKind.map(ATTRIBUTE_KIND_TO_VALUE_TYPEMAP::get).orElse(STRING);

    final IdentifierConverter identifierConverter =
        identifierConverterFactory.getIdentifierConverter(
            id, subDocPath, constant.getValue().getValueType(), requestContext);

    final IdentifierConversionMetadata metadata =
        IdentifierConversionMetadata.builder()
            .subDocPath(subDocPath)
            .operator(operator)
            .valueType(valueType)
            .build();
    final String suffixedSubDocPath = identifierConverter.convert(metadata, requestContext);

    final IdentifierExpression identifierExpression = IdentifierExpression.of(suffixedSubDocPath);
    final RelationalOperator relationalOperator = convertOperator(operator);
    final ConstantExpression constantExpression =
        constantExpressionConverter.convert(constant, requestContext);

    return RelationalExpression.of(identifierExpression, relationalOperator, constantExpression);
  }
}
