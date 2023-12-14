package org.hypertrace.entity.attribute.translator;

import static org.hypertrace.entity.attribute.translator.EntityAttributeMapping.ENTITY_ATTRIBUTE_DOC_PREFIX;

import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.query.service.v1.AttributeUpdateOperation;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.UpdateOperation;

@Slf4j
public class EntityAttributeChangeEvaluator {

  private static final String SKIP_ATTRIBUTES_CONFIG_PATH = "entity.service.change.skip.attributes";
  private static final String ENTITY_SERVICE_CHANGE_ENABLED_ENTITY_TYPES_CONFIG =
      "entity.service.change.enabled.entity.types";
  public static final String ALL_ENTITY_TYPES = "*";
  private final Set<String> allowedEntityTypes;
  private final List<String> changeNotificationSkipAttributeList;
  private final EntityAttributeMapping entityAttributeMapping;

  public EntityAttributeChangeEvaluator(
      Config appConfig, EntityAttributeMapping entityAttributeMapping) {
    this.changeNotificationSkipAttributeList = appConfig.getStringList(SKIP_ATTRIBUTES_CONFIG_PATH);
    this.allowedEntityTypes =
        appConfig.hasPath(ENTITY_SERVICE_CHANGE_ENABLED_ENTITY_TYPES_CONFIG)
            ? new HashSet<>(
                appConfig.getStringList(ENTITY_SERVICE_CHANGE_ENABLED_ENTITY_TYPES_CONFIG))
            : Set.of(ALL_ENTITY_TYPES);
    this.entityAttributeMapping = entityAttributeMapping;
  }

  public boolean shouldSendNotification(
      RequestContext requestContext, Entity prevEntity, Entity currEntity) {
    String entityType = prevEntity.getEntityType();
    if (!isEntityTypeAllowed(entityType)) {
      return false;
    }

    Entity.Builder prevEntityBuilder = prevEntity.toBuilder();
    Entity.Builder currEntityBuilder = currEntity.toBuilder();
    log.debug("PrevEntity: {}", prevEntity);
    log.debug("CurrEntity: {}", currEntity);
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
    if (!isEntityTypeAllowed(entityType)) {
      return false;
    }
    String attributeId = columnIdentifier.getColumnName();
    log.debug("AttributeId : {} for shouldSendNotification", attributeId);
    log.debug("Skipped attributes: {}", changeNotificationSkipAttributeList);
    return !this.changeNotificationSkipAttributeList.contains(attributeId);
  }

  public boolean shouldSendNotification(
      RequestContext requestContext, String entityType, UpdateOperation updateOperation) {
    if (!isEntityTypeAllowed(entityType)) {
      return false;
    }
    ColumnIdentifier columnIdentifier = updateOperation.getSetAttribute().getAttribute();
    return this.shouldSendNotification(requestContext, entityType, columnIdentifier);
  }

  public boolean shouldSendNotification(
      RequestContext requestContext, String entityType, List<UpdateOperation> updateOperations) {
    if (!isEntityTypeAllowed(entityType)) {
      log.debug(
          "Entity type : {} not allowed for change event creation. Allowed entityTypes: {}",
          entityType,
          allowedEntityTypes);
      return false;
    }
    log.debug("Update Operations: {}", updateOperations);
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

  private boolean isEntityTypeAllowed(String entityType) {
    return allowedEntityTypes.contains(ALL_ENTITY_TYPES) || allowedEntityTypes.contains(entityType);
  }

  public boolean shouldSendNotificationForAttributeUpdates(
      final RequestContext requestContext,
      final String entityType,
      final List<AttributeUpdateOperation> updateOperations) {
    if (!isEntityTypeAllowed(entityType)) {
      return false;
    }

    return updateOperations.stream()
        .map(AttributeUpdateOperation::getAttribute)
        .anyMatch(
            columnIdentifier ->
                shouldSendNotification(requestContext, entityType, columnIdentifier));
  }
}
