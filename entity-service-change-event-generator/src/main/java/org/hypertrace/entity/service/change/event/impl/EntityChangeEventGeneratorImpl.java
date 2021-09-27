package org.hypertrace.entity.service.change.event.impl;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import java.util.Map;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreProvider;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue.Builder;
import org.hypertrace.entity.change.event.v1.EntityCreateEvent;
import org.hypertrace.entity.change.event.v1.EntityDeleteEvent;
import org.hypertrace.entity.change.event.v1.EntityUpdateEvent;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;

/** The interface Entity change event generator. */
public class EntityChangeEventGeneratorImpl implements EntityChangeEventGenerator {
  private static final String EVENT_STORE = "event.store";
  private static final String EVENT_STORE_TYPE_CONFIG = "type";
  private static final String ENTITY_CHANGE_EVENTS_TOPIC = "entity-change-events";
  private static final String ENTITY_CHANGE_EVENTS_PRODUCER_CONFIG =
      "entity.change.events.producer";

  private EventStore eventStore;
  private EventProducer<EntityChangeEventKey, EntityChangeEventValue> entityChangeEventProducer;

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

  @Override
  public void sendChangeNotification(
      Map<String, Entity> existingEntityMap, Map<String, Entity> upsertedEntityMap) {
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
    Builder builder = EntityChangeEventValue.newBuilder();
    builder.setCreateEvent(EntityCreateEvent.newBuilder().setCreatedEntity(createdEntity).build());
    entityChangeEventProducer.send(getEntityChangeEventKey(createdEntity), builder.build());
  }

  private void sendUpdateNotification(Entity prevEntity, Entity currEntity) {
    Builder builder = EntityChangeEventValue.newBuilder();
    builder.setUpdateEvent(
        EntityUpdateEvent.newBuilder()
            .setPreviousEntity(prevEntity)
            .setLatestEntity(currEntity)
            .build());
    entityChangeEventProducer.send(getEntityChangeEventKey(currEntity), builder.build());
  }

  private void sendDeleteNotification(Entity deletedEntity) {
    Builder builder = EntityChangeEventValue.newBuilder();
    builder.setDeleteEvent(EntityDeleteEvent.newBuilder().setDeletedEntity(deletedEntity).build());
    entityChangeEventProducer.send(getEntityChangeEventKey(deletedEntity), builder.build());
  }

  private EntityChangeEventKey getEntityChangeEventKey(Entity entity) {
    return EntityChangeEventKey.newBuilder()
        .setTenantId(entity.getTenantId())
        .setEntityType(entity.getEntityType())
        .setEntityId(entity.getEntityId())
        .build();
  }
}
