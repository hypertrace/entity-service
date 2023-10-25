package org.hypertrace.entity.data.service;

import static java.util.function.Predicate.not;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.Key;
import org.hypertrace.core.documentstore.SingleValueKey;
import org.hypertrace.entity.data.service.EntityDataServiceImpl.ErrorMessages;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.util.StringUtils;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.entity.type.service.v1.AttributeType;

public class EntityNormalizer {
  private final EntityTypeClient entityTypeV2Client;
  private final EntityIdGenerator idGenerator;
  private final IdentifyingAttributeCache identifyingAttributeCache;

  public EntityNormalizer(
      EntityTypeClient entityTypeClient,
      EntityIdGenerator idGenerator,
      IdentifyingAttributeCache identifyingAttributeCache) {
    this.entityTypeV2Client = entityTypeClient;
    this.idGenerator = idGenerator;
    this.identifyingAttributeCache = identifyingAttributeCache;
  }

  /**
   * Normalizes the entity to a canonical, ready-to-upsert form
   *
   * @param receivedEntity
   * @throws RuntimeException If entity can not be normalized
   * @return
   */
  Entity normalize(String tenantId, Entity receivedEntity) {
    if (StringUtils.isEmpty(receivedEntity.getEntityType())) {
      throw new RuntimeException(ErrorMessages.ENTITY_TYPE_EMPTY);
    }

    // clear created time, since document-store directly adds the created time field for the entity
    receivedEntity = Entity.newBuilder(receivedEntity).clearCreatedTime().build();

    if (this.requiresIdentifyingAttributes(receivedEntity)) {
      return this.normalizeEntityByIdentifyingAttributes(tenantId, receivedEntity);
    }
    return this.normalizeEntityWithProvidedId(tenantId, receivedEntity);
  }

  Key getEntityDocKey(String tenantId, Entity entity) {
    String entityId =
        Optional.of(entity.getEntityId())
            .filter(not(String::isEmpty))
            .orElseGet(() -> this.normalize(tenantId, entity).getEntityId());

    return this.getEntityDocKey(tenantId, entity.getEntityType(), entityId);
  }

  public Key getEntityDocKey(String tenantId, String entityType, String entityId) {
    if (!entityType.isEmpty() && this.isV2Type(entityType)) {
      return new EntityV2TypeDocKey(tenantId, entityType, entityId);
    }
    return new SingleValueKey(tenantId, entityId);
  }

  private Entity normalizeEntityByIdentifyingAttributes(String tenantId, Entity receivedEntity) {
    // Validate if all identifying attributes are present in the incoming entity
    this.verifyMatchingIdentifyingAttributes(tenantId, receivedEntity);

    // UUID is generated from identifying attributes.
    String entityId =
        this.idGenerator.generateEntityId(
            tenantId, receivedEntity.getEntityType(), receivedEntity.getIdentifyingAttributesMap());

    // Copy over the identify attributes to other attributes
    return receivedEntity.toBuilder()
        .putAllAttributes(receivedEntity.getIdentifyingAttributesMap())
        .setEntityId(entityId)
        .setTenantId(tenantId)
        .build();
  }

  private Entity normalizeEntityWithProvidedId(String tenantId, Entity receivedEntity) {
    if (StringUtils.isEmpty(receivedEntity.getEntityId())) {
      throw new IllegalArgumentException(ErrorMessages.ENTITY_ID_EMPTY);
    }

    return receivedEntity.toBuilder()
        .putAllAttributes(receivedEntity.getIdentifyingAttributesMap())
        .setTenantId(tenantId)
        .build();
  }

  private boolean requiresIdentifyingAttributes(Entity entity) {
    return !this.isV2Type(entity.getEntityType());
  }

  private boolean isV2Type(String entityType) {
    return this.entityTypeV2Client
        .get(entityType)
        .map(unused -> true)
        .onErrorReturnItem(false)
        .blockingGet();
  }

  private void verifyMatchingIdentifyingAttributes(String tenantId, Entity request) {
    Set<String> idAttrNames =
        this.identifyingAttributeCache
            .getIdentifyingAttributes(tenantId, request.getEntityType())
            .stream()
            .map(AttributeType::getName)
            .collect(Collectors.toSet());

    if (idAttrNames.isEmpty()) {
      throw new IllegalArgumentException(
          String.format(
              "No identifying attributes defined for EntityType: %s", request.getEntityType()));
    }

    if (!request.getIdentifyingAttributesMap().keySet().containsAll(idAttrNames)) {
      throw new IllegalArgumentException(
          String.format(
              "Received and expected identifying attributes differ. Received: %s . Expected: %s",
              request.getIdentifyingAttributesMap().keySet(), idAttrNames));
    }
  }
}
