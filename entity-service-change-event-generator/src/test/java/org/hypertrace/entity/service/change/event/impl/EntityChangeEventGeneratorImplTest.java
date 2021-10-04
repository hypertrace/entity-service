package org.hypertrace.entity.service.change.event.impl;

import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;
import org.hypertrace.entity.change.event.v1.EntityCreateEvent;
import org.hypertrace.entity.change.event.v1.EntityDeleteEvent;
import org.hypertrace.entity.change.event.v1.EntityUpdateEvent;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.change.event.util.KeyUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityChangeEventGeneratorImplTest {

  private static final String TEST_ENTITY_TYPE = "test-entity-type";
  private static final String TEST_TENANT_ID = "test-tenant-1";

  @Mock EventProducer<String, EntityChangeEventValue> eventProducer;

  EntityChangeEventGeneratorImpl changeEventGenerator;

  @BeforeEach
  void setup() {
    changeEventGenerator = new EntityChangeEventGeneratorImpl(eventProducer);
  }

  @Test
  void sendCreateNotification() {
    List<Entity> entities = createEntities(2);
    changeEventGenerator.sendCreateNotification(entities);
    InOrder inOrderVerifier = inOrder(eventProducer);
    inOrderVerifier
        .verify(eventProducer)
        .send(
            KeyUtil.getKey(entities.get(0)),
            EntityChangeEventValue.newBuilder()
                .setCreateEvent(
                    EntityCreateEvent.newBuilder().setCreatedEntity(entities.get(0)).build())
                .build());
    inOrderVerifier
        .verify(eventProducer)
        .send(
            KeyUtil.getKey(entities.get(1)),
            EntityChangeEventValue.newBuilder()
                .setCreateEvent(
                    EntityCreateEvent.newBuilder().setCreatedEntity(entities.get(1)).build())
                .build());
  }

  @Test
  void sendDeleteNotification() {
    List<Entity> entities = createEntities(2);
    changeEventGenerator.sendDeleteNotification(entities);
    InOrder inOrderVerifier = inOrder(eventProducer);
    inOrderVerifier
        .verify(eventProducer)
        .send(
            KeyUtil.getKey(entities.get(0)),
            EntityChangeEventValue.newBuilder()
                .setDeleteEvent(
                    EntityDeleteEvent.newBuilder().setDeletedEntity(entities.get(0)).build())
                .build());
    inOrderVerifier
        .verify(eventProducer)
        .send(
            KeyUtil.getKey(entities.get(1)),
            EntityChangeEventValue.newBuilder()
                .setDeleteEvent(
                    EntityDeleteEvent.newBuilder().setDeletedEntity(entities.get(1)).build())
                .build());
  }

  @Test
  void sendChangeNotification() {
    List<Entity> prevEntities = createEntities(3);
    List<Entity> updatedEntities = createEntities(1);
    updatedEntities.add(prevEntities.get(0));
    updatedEntities.add(prevEntities.get(1).toBuilder().setEntityName("Updated Entity").build());
    changeEventGenerator.sendChangeNotification(prevEntities, updatedEntities);
    verify(eventProducer, times(1))
        .send(
            KeyUtil.getKey(updatedEntities.get(0)),
            EntityChangeEventValue.newBuilder()
                .setCreateEvent(
                    EntityCreateEvent.newBuilder().setCreatedEntity(updatedEntities.get(0)).build())
                .build());
    verify(eventProducer, times(1))
        .send(
            KeyUtil.getKey(updatedEntities.get(2)),
            EntityChangeEventValue.newBuilder()
                .setUpdateEvent(
                    EntityUpdateEvent.newBuilder()
                        .setPreviousEntity(prevEntities.get(1))
                        .setLatestEntity(updatedEntities.get(2))
                        .build())
                .build());
    verify(eventProducer, times(1))
        .send(
            KeyUtil.getKey(prevEntities.get(2)),
            EntityChangeEventValue.newBuilder()
                .setDeleteEvent(
                    EntityDeleteEvent.newBuilder().setDeletedEntity(prevEntities.get(2)).build())
                .build());
  }

  private List<Entity> createEntities(int count) {
    return IntStream.rangeClosed(1, count)
        .mapToObj(
            i ->
                Entity.newBuilder()
                    .setTenantId(TEST_TENANT_ID)
                    .setEntityType(TEST_ENTITY_TYPE)
                    .setEntityId(UUID.randomUUID().toString())
                    .setEntityName("Test entity " + i)
                    .build())
        .collect(Collectors.toList());
  }
}
