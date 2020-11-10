package org.hypertrace.entity.data.service;

import java.util.Set;
import java.util.stream.Collectors;
import org.hypertrace.entity.data.service.EntityDataServiceImpl.ErrorMessages;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.util.StringUtils;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.entity.type.service.v1.AttributeType;

class EntityNormalizer {
  private final EntityTypeClient entityTypeV2Client;
  private final EntityIdGenerator idGenerator;
  private final IdentifyingAttributeCache identifyingAttributeCache;

  EntityNormalizer(
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

    if (this.requiresIdentifyingAttributes(receivedEntity)) {
      return this.normalizeEntityByIdentifyingAttributes(tenantId, receivedEntity);
    }
    return this.normalizeEntityWithProvidedId(tenantId, receivedEntity);
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
    return receivedEntity.toBuilder()
        .putAllAttributes(receivedEntity.getIdentifyingAttributesMap())
        .setTenantId(tenantId)
        .build();
  }

  private boolean requiresIdentifyingAttributes(Entity entity) {
    return this.entityTypeV2Client
        .get(entity.getEntityType())
        .map(
            unused ->
                entity
                    .getEntityId()
                    .isEmpty()) // If entity type is present, we require only if entity id is empty
        .onErrorReturnItem(true)
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

    if (!idAttrNames.equals(request.getIdentifyingAttributesMap().keySet())) {
      throw new IllegalArgumentException(
          String.format(
              "Received and expected identifying attributes differ. Received: %s . Expected: %s",
              request.getIdentifyingAttributesMap().keySet(), idAttrNames));
    }
  }
}
