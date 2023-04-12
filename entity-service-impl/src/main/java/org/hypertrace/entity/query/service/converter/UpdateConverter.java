package org.hypertrace.entity.query.service.converter;

import static java.util.Map.entry;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BOOL;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BYTES;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_DOUBLE;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_INT64;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING_ARRAY;
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
import java.util.Set;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
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

  private static final Map<ValueType, AttributeKind> VALUE_TYPE_TO_ATTRIBUTE_KIND_MAP =
      Map.ofEntries(
          entry(STRING, TYPE_STRING),
          entry(LONG, TYPE_INT64),
          entry(INT, TYPE_INT64),
          entry(FLOAT, TYPE_DOUBLE),
          entry(DOUBLE, TYPE_DOUBLE),
          entry(BYTES, TYPE_BYTES),
          entry(BOOL, TYPE_BOOL),
          entry(STRING_ARRAY, TYPE_STRING_ARRAY));

  private static final Map<AttributeUpdateOperator, UpdateOperator> OPERATOR_MAP =
      Map.ofEntries(
          entry(ATTRIBUTE_UPDATE_OPERATOR_SET, SET),
          entry(ATTRIBUTE_UPDATE_OPERATOR_UNSET, UNSET),
          entry(ATTRIBUTE_UPDATE_OPERATOR_ADD_TO_LIST_IF_ABSENT, ADD_TO_LIST_IF_ABSENT),
          entry(ATTRIBUTE_UPDATE_OPERATOR_REMOVE_FROM_LIST, REMOVE_ALL_FROM_LIST));

  private static final Set<AttributeUpdateOperator> ARRAY_OPERATORS =
      Set.of(
          ATTRIBUTE_UPDATE_OPERATOR_ADD_TO_LIST_IF_ABSENT,
          ATTRIBUTE_UPDATE_OPERATOR_REMOVE_FROM_LIST);

  private final EntityAttributeMapping entityAttributeMapping;
  private final ValueHelper valueHelper;

  @Override
  public SubDocumentUpdate convert(
      final AttributeUpdateOperation operation, final RequestContext context)
      throws ConversionException {
    final UpdateOperator operator = OPERATOR_MAP.get(operation.getOperator());

    if (operator == null) {
      throw new ConversionException(
          String.format("Operator %s is not supported yet", operation.getOperator()));
    }

    final String id = operation.getAttribute().getColumnName();
    final AttributeKind attributeKind =
        entityAttributeMapping
            .getAttributeKind(context, id)
            .orElseThrow(
                () ->
                    new ConversionException(
                        String.format("Cannot update non-attribute value for %s", id)));
    // Validate if the operator is applicable for the LHS type
    validateOperator(attributeKind, operation.getOperator());

    final Value value = operation.getValue().getValue();
    // Validate if the LHS and the RHS are of same type
    validateDataType(attributeKind, operation.getOperator(), value.getValueType());

    final String subDocPath =
        entityAttributeMapping.getDocStorePathByAttributeId(context, id).orElseThrow();
    final String suffixedSubDocPath;

    if (entityAttributeMapping.isArray(attributeKind)) {
      suffixedSubDocPath = DOT_JOINER.join(subDocPath, VALUE_LIST_KEY, VALUES_KEY);
    } else {
      suffixedSubDocPath = subDocPath;
    }

    final SubDocumentUpdateBuilder updateBuilder =
        SubDocumentUpdate.builder()
            .subDocument(SubDocument.builder().path(suffixedSubDocPath).build())
            .operator(operator);

    if (operator != UNSET) {
      final SubDocumentValue subDocValue =
          valueHelper.convertToSubDocumentValue(attributeKind, value);
      updateBuilder.subDocumentValue(subDocValue);
    }

    return updateBuilder.build();
  }

  private void validateDataType(
      final AttributeKind attributeKind,
      final AttributeUpdateOperator operator,
      final ValueType valueType)
      throws ConversionException {
    if (operator == ATTRIBUTE_UPDATE_OPERATOR_UNSET) {
      // value type is immaterial in the case of UNSET operation
      return;
    }

    if (!attributeKind.equals(VALUE_TYPE_TO_ATTRIBUTE_KIND_MAP.get(valueType))) {
      throw new ConversionException(
          String.format(
              "Mismatching value type (%s) for attribute of type %s", valueType, attributeKind));
    }
  }

  private void validateOperator(
      final AttributeKind attributeKind, final AttributeUpdateOperator operator)
      throws ConversionException {
    if (!entityAttributeMapping.isArray(attributeKind) && ARRAY_OPERATORS.contains(operator)) {
      throw new ConversionException(
          String.format(
              "Cannot perform array operation (%s) on an attribute of type %s",
              operator, attributeKind));
    }
  }
}
