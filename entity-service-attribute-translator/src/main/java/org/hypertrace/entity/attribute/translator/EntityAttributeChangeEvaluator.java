package org.hypertrace.entity.attribute.translator;

import static org.hypertrace.entity.attribute.translator.EntityAttributeMapping.ENTITY_ATTRIBUTE_DOC_PREFIX;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.UpdateOperation;

public class EntityAttributeChangeEvaluator {

  private static final String SKIP_ATTRIBUTES_CONFIG_PATH = "entity.service.change.skip.attributes";
  private final List<String> changeNotificationSkipAttributeList;
  private final EntityAttributeMapping entityAttributeMapping;

  public EntityAttributeChangeEvaluator(
      Config appConfig, EntityAttributeMapping entityAttributeMapping) {
    this.changeNotificationSkipAttributeList = appConfig.getStringList(SKIP_ATTRIBUTES_CONFIG_PATH);
    this.entityAttributeMapping = entityAttributeMapping;
  }

  public boolean shouldSendNotification(
      RequestContext requestContext, Entity prevEntity, Entity currEntity) {
    Entity.Builder prevEntityBuilder = prevEntity.toBuilder();
    Entity.Builder currEntityBuilder = currEntity.toBuilder();
    String entityType = prevEntityBuilder.getEntityType();
    this.changeNotificationSkipAttributeList.forEach(
        attributeId ->
            removeAttributeFromEntity(
                requestContext, attributeId, entityType, prevEntityBuilder, currEntityBuilder));

    return !Maps.difference(
            prevEntityBuilder.build().getAttributesMap(),
            currEntityBuilder.build().getAttributesMap())
        .areEqual();
  }

  public boolean shouldSendNotification(
      RequestContext requestContext, ColumnIdentifier columnIdentifier) {
    String attributeId = columnIdentifier.getColumnName();
    return !this.changeNotificationSkipAttributeList.contains(attributeId);
  }

  public boolean shouldSendNotification(
      RequestContext requestContext, UpdateOperation updateOperation) {
    ColumnIdentifier columnIdentifier = updateOperation.getSetAttribute().getAttribute();
    return this.shouldSendNotification(requestContext, columnIdentifier);
  }

  public boolean shouldSendNotification(
      RequestContext requestContext, List<UpdateOperation> updateOperations) {
    List<UpdateOperation> validUpdateOperations =
        updateOperations.stream()
            .filter(UpdateOperation::hasSetAttribute)
            .filter(updateOperation -> this.shouldSendNotification(requestContext, updateOperation))
            .collect(Collectors.toUnmodifiableList());
    return !validUpdateOperations.isEmpty();
  }

  private void removeAttributeFromEntity(
      RequestContext requestContext,
      String attributeId,
      String entityType,
      Entity.Builder prevEntityBuilder,
      Entity.Builder currEntityBuilder) {
    Optional<AttributeMetadataIdentifier> metadataIdentifier =
        this.entityAttributeMapping.getAttributeMetadataByAttributeId(requestContext, attributeId);

    metadataIdentifier.ifPresent(
        metadata -> {
          if (metadata.getScope().equals(entityType)) {
            String docStorePath = metadata.getDocStorePath();
            String attributeName = removePrefix(docStorePath, ENTITY_ATTRIBUTE_DOC_PREFIX);
            prevEntityBuilder.removeAttributes(attributeName);
            currEntityBuilder.removeAttributes(attributeName);
          }
        });
  }

  private String removePrefix(String str, final String prefix) {
    if (str != null && prefix != null && str.startsWith(prefix)) {
      return str.substring(prefix.length());
    }
    return str;
  }
}
