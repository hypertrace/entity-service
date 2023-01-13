package org.hypertrace.entity.query.service.converter;

import static java.util.Collections.emptySet;
import static java.util.Map.entry;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.ADD_TO_LIST_IF_ABSENT;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.REMOVE_ALL_FROM_LIST;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.SET;
import static org.hypertrace.core.documentstore.model.subdoc.UpdateOperator.UNSET;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUES_KEY;
import static org.hypertrace.entity.query.service.converter.ValueHelper.VALUE_LIST_KEY;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_ADD_TO_LIST_IF_ABSENT;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_REMOVE_FROM_LIST;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_SET;
import static org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator.ATTRIBUTE_UPDATE_OPERATOR_UNSET;
import static org.hypertrace.entity.query.service.v1.ValueType.BOOL;
import static org.hypertrace.entity.query.service.v1.ValueType.BYTES;
import static org.hypertrace.entity.query.service.v1.ValueType.DOUBLE;
import static org.hypertrace.entity.query.service.v1.ValueType.FLOAT;
import static org.hypertrace.entity.query.service.v1.ValueType.INT;
import static org.hypertrace.entity.query.service.v1.ValueType.LONG;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING;
import static org.hypertrace.entity.query.service.v1.ValueType.STRING_ARRAY;

import com.google.common.base.Joiner;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.experimental.Accessors;
import org.hypertrace.core.documentstore.model.subdoc.SubDocument;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentUpdate.SubDocumentUpdateBuilder;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.documentstore.model.subdoc.UpdateOperator;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation.AttributeUpdateOperator;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

@AllArgsConstructor(onConstructor_ = {@Inject})
public class UpdateConverter implements Converter<AttributeUpdateOperation, SubDocumentUpdate> {
  private static final Joiner DOT_JOINER = Joiner.on(".");

  private static final Set<ValueType> ALL_SUPPORTED_TYPES =
      Set.of(STRING, LONG, INT, FLOAT, DOUBLE, BYTES, BOOL, STRING_ARRAY);
  private static final Set<ValueType> PRIMITIVE_TYPES =
      Set.of(STRING, LONG, INT, FLOAT, DOUBLE, BYTES, BOOL);
  private static final Set<ValueType> ARRAY_TYPES = Set.of(STRING_ARRAY);

  private static final Map<AttributeUpdateOperator, AttributeOperationMetadata> OPERATOR_MAP =
      Map.ofEntries(
          entry(
              ATTRIBUTE_UPDATE_OPERATOR_SET,
              AttributeOperationMetadata.builder()
                  .operator(SET)
                  .allowedValueTypesForSingleValuedAttribute(PRIMITIVE_TYPES)
                  .allowedValueTypesForMultiValuedAttribute(ARRAY_TYPES)
                  .build()),
          entry(
              ATTRIBUTE_UPDATE_OPERATOR_UNSET,
              AttributeOperationMetadata.builder()
                  .operator(UNSET)
                  .allowedValueTypesForSingleValuedAttribute(ALL_SUPPORTED_TYPES)
                  .allowedValueTypesForMultiValuedAttribute(ALL_SUPPORTED_TYPES)
                  .build()),
          entry(
              ATTRIBUTE_UPDATE_OPERATOR_ADD_TO_LIST_IF_ABSENT,
              AttributeOperationMetadata.builder()
                  .operator(ADD_TO_LIST_IF_ABSENT)
                  .allowedValueTypesForSingleValuedAttribute(emptySet())
                  .allowedValueTypesForMultiValuedAttribute(ALL_SUPPORTED_TYPES)
                  .build()),
          entry(
              ATTRIBUTE_UPDATE_OPERATOR_REMOVE_FROM_LIST,
              AttributeOperationMetadata.builder()
                  .operator(REMOVE_ALL_FROM_LIST)
                  .allowedValueTypesForSingleValuedAttribute(emptySet())
                  .allowedValueTypesForMultiValuedAttribute(ALL_SUPPORTED_TYPES)
                  .build()));

  private final EntityAttributeMapping entityAttributeMapping;
  private final ValueHelper valueHelper;

  @Override
  public SubDocumentUpdate convert(
      final AttributeUpdateOperation operation, final RequestContext context)
      throws ConversionException {
    final AttributeOperationMetadata metadata = OPERATOR_MAP.get(operation.getOperator());

    if (metadata == null) {
      throw new ConversionException(
          String.format("Operator %s is not supported yet", operation.getOperator()));
    }

    final String id = operation.getAttribute().getColumnName();
    final Optional<String> pathOptional =
        entityAttributeMapping.getDocStorePathByAttributeId(context, id);

    if (pathOptional.isEmpty()) {
      throw new ConversionException(String.format("Cannot update non-attribute value for %s", id));
    }

    final String subDocPath = pathOptional.orElseThrow();
    final UpdateOperator operator = metadata.operator();
    final Value value = operation.getValue().getValue();
    final String suffixedSubDocPath;

    if (entityAttributeMapping.isMultiValued(context, id)) {
      validateDataType(
          "ARRAY",
          operation.getOperator(),
          value.getValueType(),
          metadata.allowedValueTypesForMultiValuedAttribute());
      suffixedSubDocPath = DOT_JOINER.join(subDocPath, VALUE_LIST_KEY, VALUES_KEY);
    } else {
      validateDataType(
          "PRIMITIVE",
          operation.getOperator(),
          value.getValueType(),
          metadata.allowedValueTypesForSingleValuedAttribute());
      suffixedSubDocPath = subDocPath;
    }

    final SubDocumentUpdateBuilder updateBuilder =
        SubDocumentUpdate.builder()
            .subDocument(SubDocument.builder().path(suffixedSubDocPath).build())
            .operator(operator);

    if (operator != UNSET) {
      final SubDocumentValue subDocValue = valueHelper.convertToSubDocumentValue(value);
      updateBuilder.subDocumentValue(subDocValue);
    }

    return updateBuilder.build();
  }

  private void validateDataType(
      final String lhsType,
      final AttributeUpdateOperator operator,
      final ValueType valueType,
      final Set<ValueType> allowedValueTypes)
      throws ConversionException {
    if (!allowedValueTypes.contains(valueType)) {
      throw new ConversionException(
          String.format(
              "Invalid data type: [LHS=%s, OPERATOR=%s, RHS=%s]", lhsType, operator, valueType));
    }
  }

  @lombok.Value
  @Accessors(fluent = true)
  @Builder
  private static class AttributeOperationMetadata {
    UpdateOperator operator;
    Set<ValueType> allowedValueTypesForMultiValuedAttribute;
    Set<ValueType> allowedValueTypesForSingleValuedAttribute;
  }
}
