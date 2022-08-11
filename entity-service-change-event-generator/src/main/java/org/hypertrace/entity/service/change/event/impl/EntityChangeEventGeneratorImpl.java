package org.hypertrace.entity.service.change.event.impl;

import static java.util.function.Function.identity;
import static org.hypertrace.entity.attribute.translator.EntityAttributeMapping.ENTITY_ATTRIBUTE_DOC_PREFIX;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreProvider;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.AttributeMetadata;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue.Builder;
import org.hypertrace.entity.change.event.v1.EntityCreateEvent;
import org.hypertrace.entity.change.event.v1.EntityDeleteEvent;
import org.hypertrace.entity.change.event.v1.EntityUpdateEvent;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;
import org.hypertrace.entity.service.change.event.util.KeyUtil;

/** The interface Entity change event generator. */
@Slf4j
public class EntityChangeEventGeneratorImpl implements EntityChangeEventGenerator {

  private static final String SKIP_ATTRIBUTES_CONFIG_PATH = "entity.service.change.skip.attributes";
  private static final String EVENT_STORE = "event.store";
  private static final String EVENT_STORE_TYPE_CONFIG = "type";
  private static final String ENTITY_CHANGE_EVENTS_TOPIC = "entity-change-events";
  private static final String ENTITY_CHANGE_EVENTS_PRODUCER_CONFIG =
      "entity.change.events.producer";

  private final EventProducer<EntityChangeEventKey, EntityChangeEventValue>
      entityChangeEventProducer;
  private final List<String> changeNotificationSkipAttributeList;
  private final EntityAttributeMapping entityAttributeMapping;
  private final Clock clock;

  EntityChangeEventGeneratorImpl(
      Config appConfig, EntityAttributeMapping entityAttributeMapping, Clock clock) {
    Config config = appConfig.getConfig(EVENT_STORE);
    this.changeNotificationSkipAttributeList = appConfig.getStringList(SKIP_ATTRIBUTES_CONFIG_PATH);
    this.entityAttributeMapping = entityAttributeMapping;
    this.clock = clock;
    String storeType = config.getString(EVENT_STORE_TYPE_CONFIG);
    EventStore eventStore = EventStoreProvider.getEventStore(storeType, config);
    entityChangeEventProducer =
        eventStore.createProducer(
            ENTITY_CHANGE_EVENTS_TOPIC,
            new EventProducerConfig(
                storeType, config.getConfig(ENTITY_CHANGE_EVENTS_PRODUCER_CONFIG)));
  }

  @VisibleForTesting
  EntityChangeEventGeneratorImpl(
      EventProducer<EntityChangeEventKey, EntityChangeEventValue> entityChangeEventProducer,
      List<String> changeNotificationSkipAttributeList,
      EntityAttributeMapping entityAttributeMapping,
      Clock clock) {
    this.clock = clock;
    this.entityChangeEventProducer = entityChangeEventProducer;
    this.changeNotificationSkipAttributeList = changeNotificationSkipAttributeList;
    this.entityAttributeMapping = entityAttributeMapping;
  }

  @Override
  public void sendCreateNotification(RequestContext requestContext, Collection<Entity> entities) {
    entities.forEach(entity -> this.sendCreateNotification(requestContext, entity));
  }

  @Override
  public void sendDeleteNotification(RequestContext requestContext, Collection<Entity> entities) {
    entities.forEach(entity -> this.sendDeleteNotification(requestContext, entity));
  }

  @Override
  public void sendChangeNotification(
      RequestContext requestContext,
      Collection<Entity> existingEntities,
      Collection<Entity> updatedEntities) {
    Map<String, Entity> existingEntityMap =
        existingEntities.stream().collect(Collectors.toMap(Entity::getEntityId, identity()));
    Map<String, Entity> upsertedEntityMap =
        updatedEntities.stream().collect(Collectors.toMap(Entity::getEntityId, identity()));
    MapDifference<String, Entity> mapDifference =
        Maps.difference(existingEntityMap, upsertedEntityMap);

    mapDifference
        .entriesOnlyOnRight()
        .entrySet()
        .forEach(
            entry -> {
              sendCreateNotification(requestContext, entry.getValue());
            });

    mapDifference
        .entriesDiffering()
        .entrySet()
        .forEach(
            entry -> {
              MapDifference.ValueDifference<Entity> valueDifference = entry.getValue();
              Entity prevEntity = valueDifference.leftValue();
              Entity currEntity = valueDifference.rightValue();
              sendUpdateNotificationIfRequired(requestContext, prevEntity, currEntity);
            });

    mapDifference
        .entriesOnlyOnLeft()
        .entrySet()
        .forEach(
            entry -> {
              sendDeleteNotification(requestContext, entry.getValue());
            });
  }

  private void sendCreateNotification(RequestContext requestContext, Entity createdEntity) {
    try {
      Builder builder = EntityChangeEventValue.newBuilder();
      builder.setCreateEvent(
          EntityCreateEvent.newBuilder().setCreatedEntity(createdEntity).build());
      builder.setEventTimeMillis(clock.millis());
      populateUserDetails(requestContext, builder);
      entityChangeEventProducer.send(getEntityChangeEventKey(createdEntity), builder.build());
    } catch (Exception ex) {
      log.warn(
          "Unable to send create event for entity with id {} for tenant {}",
          createdEntity.getEntityId(),
          createdEntity.getTenantId(),
          ex);
    }
  }

  private void sendUpdateNotificationIfRequired(
      RequestContext requestContext, Entity prevEntity, Entity currEntity) {
    try {
      if (!shouldSendNotification(requestContext, prevEntity, currEntity)) {
        return;
      }

      Builder builder = EntityChangeEventValue.newBuilder();
      builder.setUpdateEvent(
          EntityUpdateEvent.newBuilder()
              .setPreviousEntity(prevEntity)
              .setLatestEntity(currEntity)
              .build());
      builder.setEventTimeMillis(clock.millis());
      populateUserDetails(requestContext, builder);
      entityChangeEventProducer.send(getEntityChangeEventKey(currEntity), builder.build());
    } catch (Exception ex) {
      log.warn(
          "Unable to send update event for entity with id {} for tenant {}",
          currEntity.getEntityId(),
          currEntity.getTenantId(),
          ex);
    }
  }

  private boolean shouldSendNotification(
      RequestContext requestContext, Entity prevEntity, Entity currEntity) {
    String entityType = prevEntity.getEntityType();
    Entity.Builder prevEntityBuilder = prevEntity.toBuilder();
    Entity.Builder currEntityBuilder = currEntity.toBuilder();
    this.changeNotificationSkipAttributeList.forEach(
        attributeId -> {
          Optional<AttributeMetadata> attributeScopeKey =
              this.entityAttributeMapping.getAttributeMetadataByAttributeId(requestContext, attributeId);
          if (attributeScopeKey.isPresent()
              && attributeScopeKey.get().getScope().equals(entityType)) {
            String docStorePath = attributeScopeKey.get().getDocStorePath();
            String attributeName = removePrefix(docStorePath, ENTITY_ATTRIBUTE_DOC_PREFIX);
            prevEntityBuilder.removeAttributes(attributeName);
            currEntityBuilder.removeAttributes(attributeName);
          }
        });

    MapDifference<String, AttributeValue> attributesDiff =
        Maps.difference(
            prevEntityBuilder.build().getAttributesMap(),
            currEntityBuilder.build().getAttributesMap());

    return !attributesDiff.areEqual();
  }

  private void sendDeleteNotification(RequestContext requestContext, Entity deletedEntity) {
    try {
      Builder builder = EntityChangeEventValue.newBuilder();
      builder.setDeleteEvent(
          EntityDeleteEvent.newBuilder().setDeletedEntity(deletedEntity).build());
      builder.setEventTimeMillis(clock.millis());
      populateUserDetails(requestContext, builder);
      entityChangeEventProducer.send(getEntityChangeEventKey(deletedEntity), builder.build());
    } catch (Exception ex) {
      log.warn(
          "Unable to send delete event for entity with id {} for tenant {}",
          deletedEntity.getEntityId(),
          deletedEntity.getTenantId(),
          ex);
    }
  }

  private void populateUserDetails(RequestContext requestContext, Builder builder) {
    requestContext.getUserId().ifPresent(userId -> builder.setUserId(userId));
    requestContext.getName().ifPresent(userName -> builder.setUserName(userName));
  }

  private EntityChangeEventKey getEntityChangeEventKey(Entity entity) {
    return KeyUtil.getKey(entity);
  }

  private String removePrefix(String str, final String prefix) {
    if (str != null && prefix != null && str.startsWith(prefix)) {
      return str.substring(prefix.length());
    }
    return str;
  }
}
