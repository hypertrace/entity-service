package org.hypertrace.entity.service.change.event.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.eventstore.EventStore;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityChangeEventGeneratorImplTest {

  private static final String TEST_ENTITY_TYPE = "test-entity-type";
  private static final String TEST_TENANT_ID = "test-tenant-1";

  @Mock EventStore eventStore;

  @Mock EventProducer<EntityChangeEventKey, EntityChangeEventValue> eventProducer;

  EntityChangeEventGeneratorImpl changeEventGenerator;

  @BeforeEach
  void setup() {
    changeEventGenerator = new EntityChangeEventGeneratorImpl(eventStore, eventProducer);
  }

  @Test
  void sendCreateNotification() {
    List<Entity> entities = createEntities(2);
    ArgumentCaptor<EntityChangeEventKey> keyCaptor =
        ArgumentCaptor.forClass(EntityChangeEventKey.class);
    ArgumentCaptor<EntityChangeEventValue> valueCaptor =
        ArgumentCaptor.forClass(EntityChangeEventValue.class);
    changeEventGenerator.sendCreateNotification(entities);
    verify(eventProducer, times(2)).send(keyCaptor.capture(), valueCaptor.capture());
    AtomicInteger index = new AtomicInteger(0);
    keyCaptor
        .getAllValues()
        .forEach(
            key -> {
              assertEquals(TEST_TENANT_ID, key.getTenantId());
              assertEquals(TEST_ENTITY_TYPE, key.getEntityType());
              assertEquals(entities.get(index.getAndIncrement()).getEntityId(), key.getEntityId());
            });
    index.set(0);
    valueCaptor
        .getAllValues()
        .forEach(
            value -> {
              assertNotNull(value.getCreateEvent());
              assertEquals(
                  entities.get(index.getAndIncrement()), value.getCreateEvent().getCreatedEntity());
            });
  }

  @Test
  void sendDeleteNotification() {
    List<Entity> entities = createEntities(2);
    ArgumentCaptor<EntityChangeEventKey> keyCaptor =
        ArgumentCaptor.forClass(EntityChangeEventKey.class);
    ArgumentCaptor<EntityChangeEventValue> valueCaptor =
        ArgumentCaptor.forClass(EntityChangeEventValue.class);
    changeEventGenerator.sendDeleteNotification(entities);
    verify(eventProducer, times(2)).send(keyCaptor.capture(), valueCaptor.capture());
    AtomicInteger index = new AtomicInteger(0);
    keyCaptor
        .getAllValues()
        .forEach(
            key -> {
              assertEquals(TEST_TENANT_ID, key.getTenantId());
              assertEquals(TEST_ENTITY_TYPE, key.getEntityType());
              assertEquals(entities.get(index.getAndIncrement()).getEntityId(), key.getEntityId());
            });
    index.set(0);
    valueCaptor
        .getAllValues()
        .forEach(
            value -> {
              assertNotNull(value.getCreateEvent());
              assertEquals(
                  entities.get(index.getAndIncrement()), value.getDeleteEvent().getDeletedEntity());
            });
  }

  @Test
  void sendChangeNotification() {
    List<Entity> prevEntities = createEntities(3);
    List<Entity> updatedEntities = createEntities(1);
    updatedEntities.add(prevEntities.get(0));
    updatedEntities.add(prevEntities.get(1).toBuilder().setEntityName("Updated Entity").build());

    ArgumentCaptor<EntityChangeEventKey> keyCaptor =
        ArgumentCaptor.forClass(EntityChangeEventKey.class);
    ArgumentCaptor<EntityChangeEventValue> valueCaptor =
        ArgumentCaptor.forClass(EntityChangeEventValue.class);
    changeEventGenerator.sendChangeNotification(prevEntities, updatedEntities);
    verify(eventProducer, times(3)).send(keyCaptor.capture(), valueCaptor.capture());

    keyCaptor
        .getAllValues()
        .forEach(
            key -> {
              assertEquals(TEST_TENANT_ID, key.getTenantId());
              assertEquals(TEST_ENTITY_TYPE, key.getEntityType());
            });

    IntStream.rangeClosed(0, 2)
        .forEach(
            index -> {
              String entityId = keyCaptor.getAllValues().get(index).getEntityId();
              EntityChangeEventValue value = valueCaptor.getAllValues().get(index);
              if (entityId.equals(updatedEntities.get(0).getEntityId())) {
                assertNotNull(value.getCreateEvent());
                assertEquals(updatedEntities.get(0), value.getCreateEvent().getCreatedEntity());
              } else if (entityId.equals(prevEntities.get(1).getEntityId())) {
                assertNotNull(value.getUpdateEvent());
                assertEquals(prevEntities.get(1), value.getUpdateEvent().getPreviousEntity());
                assertEquals(updatedEntities.get(2), value.getUpdateEvent().getLatestEntity());
              } else if (entityId.equals(prevEntities.get(2).getEntityId())) {
                assertNotNull(value.getDeleteEvent());
                assertEquals(prevEntities.get(2), value.getDeleteEvent().getDeletedEntity());
              } else {
                fail();
              }
            });
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
