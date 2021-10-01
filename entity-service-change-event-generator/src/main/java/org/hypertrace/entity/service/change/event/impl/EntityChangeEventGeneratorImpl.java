package org.hypertrace.entity.service.change.event.impl;

import static java.util.function.Function.identity;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreProvider;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue.Builder;
import org.hypertrace.entity.change.event.v1.EntityCreateEvent;
import org.hypertrace.entity.change.event.v1.EntityDeleteEvent;
import org.hypertrace.entity.change.event.v1.EntityUpdateEvent;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;
import org.hypertrace.entity.service.change.event.util.KeyUtil;

/** The interface Entity change event generator. */
@Slf4j
public class EntityChangeEventGeneratorImpl implements EntityChangeEventGenerator {
  private static final String EVENT_STORE = "event.store";
  private static final String EVENT_STORE_TYPE_CONFIG = "type";
  private static final String ENTITY_CHANGE_EVENTS_TOPIC = "entity-change-events";
  private static final String ENTITY_CHANGE_EVENTS_PRODUCER_CONFIG =
      "entity.change.events.producer";

  private EventStore eventStore;
  private EventProducer<String, EntityChangeEventValue> entityChangeEventProducer;

  public EntityChangeEventGeneratorImpl(Config appConfig) {
    Config config = appConfig.getConfig(EVENT_STORE);
    String storeType = config.getString(EVENT_STORE_TYPE_CONFIG);
    eventStore = EventStoreProvider.getEventStore(storeType, config);
    entityChangeEventProducer =
        eventStore.createProducer(
            ENTITY_CHANGE_EVENTS_TOPIC,
            new EventProducerConfig(
                storeType, config.getConfig(ENTITY_CHANGE_EVENTS_PRODUCER_CONFIG)));
  }

  @VisibleForTesting
  EntityChangeEventGeneratorImpl(
      EventStore eventStore,
      EventProducer<String, EntityChangeEventValue> entityChangeEventProducer) {
    this.eventStore = eventStore;
    this.entityChangeEventProducer = entityChangeEventProducer;
  }

  @Override
  public void sendCreateNotification(Collection<Entity> entities) {
    entities.forEach(this::sendCreateNotification);
  }

  @Override
  public void sendDeleteNotification(Collection<Entity> entities) {
    entities.forEach(this::sendDeleteNotification);
  }

  @Override
  public void sendChangeNotification(
      Collection<Entity> existingEntities, Collection<Entity> updatedEntities) {
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
              sendCreateNotification(entry.getValue());
            });

    mapDifference
        .entriesDiffering()
        .entrySet()
        .forEach(
            entry -> {
              MapDifference.ValueDifference<Entity> valueDifference = entry.getValue();
              Entity prevEntity = valueDifference.leftValue();
              Entity currEntity = valueDifference.rightValue();
              sendUpdateNotification(prevEntity, currEntity);
            });

    mapDifference
        .entriesOnlyOnLeft()
        .entrySet()
        .forEach(
            entry -> {
              sendDeleteNotification(entry.getValue());
            });
  }

  private void sendCreateNotification(Entity createdEntity) {
    try {
      Builder builder = EntityChangeEventValue.newBuilder();
      builder.setCreateEvent(
          EntityCreateEvent.newBuilder().setCreatedEntity(createdEntity).build());
      entityChangeEventProducer.send(getEntityChangeEventKey(createdEntity), builder.build());
    } catch (Exception ex) {
      log.warn(
          "Unable to send create event for entity with id {} for tenant {}",
          createdEntity.getEntityId(),
          createdEntity.getTenantId(),
          ex);
    }
  }

  private void sendUpdateNotification(Entity prevEntity, Entity currEntity) {
    try {
      Builder builder = EntityChangeEventValue.newBuilder();
      builder.setUpdateEvent(
          EntityUpdateEvent.newBuilder()
              .setPreviousEntity(prevEntity)
              .setLatestEntity(currEntity)
              .build());
      entityChangeEventProducer.send(getEntityChangeEventKey(currEntity), builder.build());
    } catch (Exception ex) {
      log.warn(
          "Unable to send update event for entity with id {} for tenant {}",
          currEntity.getEntityId(),
          currEntity.getTenantId(),
          ex);
    }
  }

  private void sendDeleteNotification(Entity deletedEntity) {
    try {
      Builder builder = EntityChangeEventValue.newBuilder();
      builder.setDeleteEvent(
          EntityDeleteEvent.newBuilder().setDeletedEntity(deletedEntity).build());
      entityChangeEventProducer.send(getEntityChangeEventKey(deletedEntity), builder.build());
    } catch (Exception ex) {
      log.warn(
          "Unable to send delete event for entity with id {} for tenant {}",
          deletedEntity.getEntityId(),
          deletedEntity.getTenantId(),
          ex);
    }
  }

  private String getEntityChangeEventKey(Entity entity) {
    return KeyUtil.getKey(entity);
  }
}
