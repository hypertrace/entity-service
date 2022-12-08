package org.hypertrace.entity.query.service.converter;

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

import com.google.common.base.Joiner;
import java.util.Map;
import java.util.Optional;
import javax.inject.Inject;
import lombok.AllArgsConstructor;
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

@AllArgsConstructor(onConstructor_ = {@Inject})
public class UpdateConverter implements Converter<AttributeUpdateOperation, SubDocumentUpdate> {
  private static final Joiner DOT_JOINER = Joiner.on(".");

  private static final Map<AttributeUpdateOperator, UpdateOperator> OPERATOR_MAP =
      Map.ofEntries(
          entry(ATTRIBUTE_UPDATE_OPERATOR_SET, SET),
          entry(ATTRIBUTE_UPDATE_OPERATOR_UNSET, UNSET),
          entry(ATTRIBUTE_UPDATE_OPERATOR_ADD_TO_LIST_IF_ABSENT, ADD_TO_LIST_IF_ABSENT),
          entry(ATTRIBUTE_UPDATE_OPERATOR_REMOVE_FROM_LIST, REMOVE_ALL_FROM_LIST));

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
    final Optional<String> pathOptional =
        entityAttributeMapping.getDocStorePathByAttributeId(context, id);

    if (pathOptional.isEmpty()) {
      throw new ConversionException(String.format("Cannot update non-attribute value for %s", id));
    }

    final String subDocPath = pathOptional.orElseThrow();
    final Value value = operation.getValue().getValue();
    final String suffixedSubDocPath;

    if (entityAttributeMapping.isMultiValued(context, id)) {
      suffixedSubDocPath = DOT_JOINER.join(subDocPath, VALUE_LIST_KEY, VALUES_KEY);
    } else {
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
}
