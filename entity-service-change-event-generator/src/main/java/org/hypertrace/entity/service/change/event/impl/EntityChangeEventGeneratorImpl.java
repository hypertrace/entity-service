package org.hypertrace.entity.service.change.event.impl;

import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;
import java.time.Clock;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventProducerConfig;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.core.eventstore.EventStoreProvider;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeChangeEvaluator;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
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

  private final EventProducer<EntityChangeEventKey, EntityChangeEventValue>
      entityChangeEventProducer;
  private final EntityAttributeChangeEvaluator entityAttributeChangeEvaluator;
  private final Clock clock;

  EntityChangeEventGeneratorImpl(
      Config appConfig, EntityAttributeMapping entityAttributeMapping, Clock clock) {
    Config config = appConfig.getConfig(EVENT_STORE);
    this.clock = clock;
    String storeType = config.getString(EVENT_STORE_TYPE_CONFIG);
    EventStore eventStore = EventStoreProvider.getEventStore(storeType, config);
    entityChangeEventProducer =
        eventStore.createProducer(
            ENTITY_CHANGE_EVENTS_TOPIC,
            new EventProducerConfig(
                storeType, config.getConfig(ENTITY_CHANGE_EVENTS_PRODUCER_CONFIG)));
    this.entityAttributeChangeEvaluator =
        new EntityAttributeChangeEvaluator(appConfig, entityAttributeMapping);
  }

  @VisibleForTesting
  EntityChangeEventGeneratorImpl(
      Config appConfig,
      EventProducer<EntityChangeEventKey, EntityChangeEventValue> entityChangeEventProducer,
      EntityAttributeMapping entityAttributeMapping,
      Clock clock) {
    this.clock = clock;
    this.entityChangeEventProducer = entityChangeEventProducer;
    this.entityAttributeChangeEvaluator =
        new EntityAttributeChangeEvaluator(appConfig, entityAttributeMapping);
  }

  @Override
  public void sendChangeNotification(RequestContext requestContext, ChangeResult changeResult) {
    changeResult
        .getCreatedEntity()
        .forEach(
            entry -> {
              sendCreateNotification(requestContext, entry);
            });

    changeResult
        .getExistingToUpdatedEntitiesMap()
        .forEach(
            (existingEntity, updatedEntity) -> {
              sendUpdateNotificationIfRequired(requestContext, existingEntity, updatedEntity);
            });

    changeResult
        .getDeletedEntity()
        .forEach(
            entry -> {
              sendDeleteNotification(requestContext, entry);
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
      if (!this.entityAttributeChangeEvaluator.shouldSendNotification(
          requestContext, prevEntity, currEntity)) {
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
}
