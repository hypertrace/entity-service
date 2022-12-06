package org.hypertrace.entity.query.service.converter;

import static org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter.getSubDocPathById;
import static org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter.isPartOfAttributeMap;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_SET;

import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConversionMetadata;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConverterFactory;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class UpdateConverter implements Converter<AttributeUpdateOperation, SubDocumentUpdate> {
  private final EntityAttributeMapping entityAttributeMapping;
  private final IdentifierConverterFactory identifierConverterFactory;
  private final ValueHelper valueHelper;

  @Override
  public SubDocumentUpdate convert(
      final AttributeUpdateOperation operation, final RequestContext context)
      throws ConversionException {
    final String id = operation.getAttribute().getColumnName();
    final String subDocPath = getSubDocPathById(entityAttributeMapping, id, context);

    if (!isPartOfAttributeMap(subDocPath)) {
      throw new ConversionException(String.format("Cannot update non-attribute value for %s", id));
    }

    final Value value = operation.getValue().getValue();
    final ValueType valueType = value.getValueType();

    final IdentifierConverter identifierConverter =
        identifierConverterFactory.getIdentifierConverter(id, subDocPath, valueType, context);
    final String suffixedSubDocPath =
        identifierConverter.convert(
            IdentifierConversionMetadata.builder()
                .subDocPath(subDocPath)
                .valueType(valueType)
                .build(),
            context);
    final SubDocumentValue subDocValue = valueHelper.convertToSubDocumentValue(value);

    if (operation.getOperator() == ATTRIBUTE_UPDATE_OPERATOR_SET) {
      return SubDocumentUpdate.of(suffixedSubDocPath, subDocValue);
    } else {
      throw new ConversionException(
          String.format("Operator %s is not supported yet", operation.getOperator()));
    }
  }
}
