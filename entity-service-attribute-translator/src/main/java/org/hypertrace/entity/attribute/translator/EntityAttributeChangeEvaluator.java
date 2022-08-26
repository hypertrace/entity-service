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
  public static final String ALL_ENTITIES = "*";
  private final List<String> allowedEntityTypes;
  private final List<String> changeNotificationSkipAttributeList;
  private final EntityAttributeMapping entityAttributeMapping;

  public EntityAttributeChangeEvaluator(
      Config appConfig, EntityAttributeMapping entityAttributeMapping) {
    this(appConfig, List.of(ALL_ENTITIES), entityAttributeMapping);
  }

  public EntityAttributeChangeEvaluator(
      Config appConfig,
      List<String> allowedEntityTypes,
      EntityAttributeMapping entityAttributeMapping) {
    this.changeNotificationSkipAttributeList = appConfig.getStringList(SKIP_ATTRIBUTES_CONFIG_PATH);
    this.allowedEntityTypes = allowedEntityTypes;
    this.entityAttributeMapping = entityAttributeMapping;
  }

  public boolean shouldSendNotification(
      RequestContext requestContext, Entity prevEntity, Entity currEntity) {
    String entityType = prevEntity.getEntityType();
    if (!isEntityTypesAllowed(entityType)) {
      return false;
    }

    Entity.Builder prevEntityBuilder = prevEntity.toBuilder();
    Entity.Builder currEntityBuilder = currEntity.toBuilder();
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
      RequestContext requestContext, String entityType, ColumnIdentifier columnIdentifier) {
    if (!isEntityTypesAllowed(entityType)) {
      return false;
    }
    String attributeId = columnIdentifier.getColumnName();
    return !this.changeNotificationSkipAttributeList.contains(attributeId);
  }

  public boolean shouldSendNotification(
      RequestContext requestContext, String entityType, UpdateOperation updateOperation) {
    if (!isEntityTypesAllowed(entityType)) {
      return false;
    }
    ColumnIdentifier columnIdentifier = updateOperation.getSetAttribute().getAttribute();
    return this.shouldSendNotification(requestContext, entityType, columnIdentifier);
  }

  public boolean shouldSendNotification(
      RequestContext requestContext, String entityType, List<UpdateOperation> updateOperations) {
    if (!isEntityTypesAllowed(entityType)) {
      return false;
    }
    List<UpdateOperation> validUpdateOperations =
        updateOperations.stream()
            .filter(UpdateOperation::hasSetAttribute)
            .filter(
                updateOperation ->
                    this.shouldSendNotification(requestContext, entityType, updateOperation))
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

  private boolean isEntityTypesAllowed(String entityType) {
    return allowedEntityTypes.contains("*") || allowedEntityTypes.contains(entityType);
  }
}
