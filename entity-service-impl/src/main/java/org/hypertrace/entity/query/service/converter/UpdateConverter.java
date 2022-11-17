package org.hypertrace.entity.query.service.converter;

import static org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter.getSubDocPathById;

import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConversionMetadata;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConverter;
import org.hypertrace.entity.query.service.converter.identifier.IdentifierConverterFactory;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class UpdateConverter implements Converter<UpdateOperation, SubDocumentUpdate> {
  private final EntityAttributeMapping entityAttributeMapping;
  private final IdentifierConverterFactory identifierConverterFactory;
  private final ValueHelper valueHelper;

  @Override
  public SubDocumentUpdate convert(
      final UpdateOperation operation, final RequestContext requestContext)
      throws ConversionException {
    switch (operation.getOperationCase()) {
      case SETATTRIBUTE:
        return convertSetAttribute(operation.getSetAttribute(), requestContext);

      case OPERATION_NOT_SET:
        throw new ConversionException("Update operation is must");

      default:
        throw new ConversionException(
            "Unsupported updated operation: " + operation.getOperationCase());
    }
  }

  private SubDocumentUpdate convertSetAttribute(
      final SetAttribute setAttribute, final RequestContext context) throws ConversionException {
    final String id = setAttribute.getAttribute().getColumnName();
    final String subDocPath = getSubDocPathById(entityAttributeMapping, id, context);
    final Value value = setAttribute.getValue().getValue();
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

    return SubDocumentUpdate.of(suffixedSubDocPath, subDocValue);
  }
}
