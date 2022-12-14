package org.hypertrace.entity.service.change.event.impl;

import static org.hypertrace.entity.attribute.translator.EntityAttributeMapping.ENTITY_ATTRIBUTE_DOC_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hypertrace.core.eventstore.EventProducer;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.AttributeMetadataIdentifier;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.change.event.v1.EntityChangeEventKey;
import org.hypertrace.entity.change.event.v1.EntityChangeEventValue;
import org.hypertrace.entity.change.event.v1.EntityCreateEvent;
import org.hypertrace.entity.change.event.v1.EntityDeleteEvent;
import org.hypertrace.entity.change.event.v1.EntityUpdateEvent;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
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
  private static final long CURRENT_TIME_MILLIS = 1000;

  @Mock EventProducer<EntityChangeEventKey, EntityChangeEventValue> eventProducer;

  @Mock EntityAttributeMapping entityAttributeMapping;
  EntityChangeEventGeneratorImpl changeEventGenerator;
  RequestContext requestContext;
  private Clock mockClock;

  @BeforeEach
  void setup() {
    mockClock = mock(Clock.class);
    when(mockClock.millis()).thenReturn(CURRENT_TIME_MILLIS);
    Config config =
        ConfigFactory.parseMap(
            Map.of(
                "entity.service.change.skip.attributes",
                List.of(
                    TEST_ENTITY_TYPE + ".skip_attribute", TEST_ENTITY_TYPE + ".skip_attribute_1")));
    changeEventGenerator =
        new EntityChangeEventGeneratorImpl(
            config, eventProducer, entityAttributeMapping, mockClock);
    requestContext = RequestContext.forTenantId(TEST_TENANT_ID);
  }

  @Test
  void sendCreateNotification() {
    List<Entity> entities = createEntities(2);
    changeEventGenerator.sendChangeNotification(
        requestContext,
        new ChangeResult(entities, Collections.emptyMap(), Collections.emptyList()));
    InOrder inOrderVerifier = inOrder(eventProducer);
    inOrderVerifier
        .verify(eventProducer)
        .send(
            KeyUtil.getKey(entities.get(0)),
            EntityChangeEventValue.newBuilder()
                .setCreateEvent(
                    EntityCreateEvent.newBuilder().setCreatedEntity(entities.get(0)).build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());
    inOrderVerifier
        .verify(eventProducer)
        .send(
            KeyUtil.getKey(entities.get(1)),
            EntityChangeEventValue.newBuilder()
                .setCreateEvent(
                    EntityCreateEvent.newBuilder().setCreatedEntity(entities.get(1)).build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());
  }

  @Test
  void sendDeleteNotification() {
    List<Entity> entities = createEntities(2);
    changeEventGenerator.sendChangeNotification(
        requestContext,
        new ChangeResult(Collections.emptyList(), Collections.emptyMap(), entities));
    InOrder inOrderVerifier = inOrder(eventProducer);
    inOrderVerifier
        .verify(eventProducer)
        .send(
            KeyUtil.getKey(entities.get(0)),
            EntityChangeEventValue.newBuilder()
                .setDeleteEvent(
                    EntityDeleteEvent.newBuilder().setDeletedEntity(entities.get(0)).build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());
    inOrderVerifier
        .verify(eventProducer)
        .send(
            KeyUtil.getKey(entities.get(1)),
            EntityChangeEventValue.newBuilder()
                .setDeleteEvent(
                    EntityDeleteEvent.newBuilder().setDeletedEntity(entities.get(1)).build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());
  }

  @Test
  void sendChangeNotification() {
    List<Entity> prevEntities = createEntities(3);
    List<Entity> updatedEntities = createEntities(1);
    updatedEntities.add(prevEntities.get(0));
    updatedEntities.add(
        prevEntities.get(1).toBuilder()
            .putAttributes(
                "attribute_key",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value").build())
                    .build())
            .build());
    ChangeResult changeResult = EntityChangeEvaluator.evaluateChange(prevEntities, updatedEntities);
    changeEventGenerator.sendChangeNotification(requestContext, changeResult);
    verify(eventProducer, times(1))
        .send(
            KeyUtil.getKey(updatedEntities.get(0)),
            EntityChangeEventValue.newBuilder()
                .setCreateEvent(
                    EntityCreateEvent.newBuilder().setCreatedEntity(updatedEntities.get(0)).build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
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
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());
    verify(eventProducer, times(1))
        .send(
            KeyUtil.getKey(prevEntities.get(2)),
            EntityChangeEventValue.newBuilder()
                .setDeleteEvent(
                    EntityDeleteEvent.newBuilder().setDeletedEntity(prevEntities.get(2)).build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());
  }

  @Test
  void sendChangeNotification_withNewAddedAttributes() {
    when(this.entityAttributeMapping.getAttributeMetadataByAttributeId(
            any(), eq(TEST_ENTITY_TYPE + ".skip_attribute")))
        .thenReturn(
            Optional.of(
                new AttributeMetadataIdentifier(
                    TEST_ENTITY_TYPE, ENTITY_ATTRIBUTE_DOC_PREFIX + "skip_attribute")));
    List<Entity> prevEntities =
        createEntities(
            1,
            Map.of(
                "attribute_key",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value").build())
                    .build(),
                "skip_attribute",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("skip_value").build())
                    .build()));
    List<Entity> updatedEntities = new ArrayList<>();
    updatedEntities.add(
        prevEntities.get(0).toBuilder()
            .putAttributes(
                "attribute_key_1",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value").build())
                    .build())
            .build());
    ChangeResult changeResult = EntityChangeEvaluator.evaluateChange(prevEntities, updatedEntities);
    changeEventGenerator.sendChangeNotification(requestContext, changeResult);
    verify(eventProducer, times(1))
        .send(
            KeyUtil.getKey(updatedEntities.get(0)),
            EntityChangeEventValue.newBuilder()
                .setUpdateEvent(
                    EntityUpdateEvent.newBuilder()
                        .setPreviousEntity(prevEntities.get(0))
                        .setLatestEntity(updatedEntities.get(0))
                        .build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());
  }

  @Test
  void sendChangeNotification_withDeletedAttributes() {
    when(this.entityAttributeMapping.getAttributeMetadataByAttributeId(
            any(), eq(TEST_ENTITY_TYPE + ".skip_attribute")))
        .thenReturn(
            Optional.of(
                new AttributeMetadataIdentifier(
                    TEST_ENTITY_TYPE, ENTITY_ATTRIBUTE_DOC_PREFIX + "skip_attribute")));
    List<Entity> prevEntities =
        createEntities(
            1,
            Map.of(
                "attribute_key",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value").build())
                    .build(),
                "skip_attribute",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("skip_value").build())
                    .build()));
    List<Entity> updatedEntities = new ArrayList<>();

    updatedEntities.add(prevEntities.get(0).toBuilder().removeAttributes("attribute_key").build());
    ChangeResult changeResult = EntityChangeEvaluator.evaluateChange(prevEntities, updatedEntities);
    changeEventGenerator.sendChangeNotification(requestContext, changeResult);
    verify(eventProducer, times(1))
        .send(
            KeyUtil.getKey(updatedEntities.get(0)),
            EntityChangeEventValue.newBuilder()
                .setUpdateEvent(
                    EntityUpdateEvent.newBuilder()
                        .setPreviousEntity(prevEntities.get(0))
                        .setLatestEntity(updatedEntities.get(0))
                        .build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());
  }

  @Test
  void sendChangeNotification_withOnlySkipAttributes() {
    when(this.entityAttributeMapping.getAttributeMetadataByAttributeId(
            any(), eq(TEST_ENTITY_TYPE + ".skip_attribute")))
        .thenReturn(
            Optional.of(
                new AttributeMetadataIdentifier(
                    TEST_ENTITY_TYPE, ENTITY_ATTRIBUTE_DOC_PREFIX + "skip_attribute")));
    when(this.entityAttributeMapping.getAttributeMetadataByAttributeId(
            any(), eq(TEST_ENTITY_TYPE + ".skip_attribute_1")))
        .thenReturn(
            Optional.of(
                new AttributeMetadataIdentifier(
                    TEST_ENTITY_TYPE, ENTITY_ATTRIBUTE_DOC_PREFIX + "skip_attribute_1")));
    List<Entity> prevEntities =
        createEntities(
            1,
            Map.of(
                "attribute_key",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value").build())
                    .build(),
                "skip_attribute",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("skip_value").build())
                    .build()));
    // delete skip attributes only
    Entity updatedEntity =
        prevEntities.get(0).toBuilder().removeAttributes("skip_attribute").build();
    ChangeResult changeResult =
        EntityChangeEvaluator.evaluateChange(prevEntities, List.of(updatedEntity));
    changeEventGenerator.sendChangeNotification(requestContext, changeResult);
    verify(eventProducer, never())
        .send(
            KeyUtil.getKey(updatedEntity),
            EntityChangeEventValue.newBuilder()
                .setUpdateEvent(
                    EntityUpdateEvent.newBuilder()
                        .setPreviousEntity(prevEntities.get(0))
                        .setLatestEntity(updatedEntity)
                        .build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());

    // update skip attributes only
    updatedEntity =
        prevEntities.get(0).toBuilder()
            .putAttributes(
                "skip_attribute",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("skip_value").build())
                    .build())
            .build();
    changeResult = EntityChangeEvaluator.evaluateChange(prevEntities, List.of(updatedEntity));
    changeEventGenerator.sendChangeNotification(requestContext, changeResult);
    verify(eventProducer, never())
        .send(
            KeyUtil.getKey(updatedEntity),
            EntityChangeEventValue.newBuilder()
                .setUpdateEvent(
                    EntityUpdateEvent.newBuilder()
                        .setPreviousEntity(prevEntities.get(0))
                        .setLatestEntity(updatedEntity)
                        .build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());

    // add skip attributes only
    updatedEntity =
        prevEntities.get(0).toBuilder()
            .putAttributes(
                "skip_attribute_1",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value").build())
                    .build())
            .build();
    changeResult = EntityChangeEvaluator.evaluateChange(prevEntities, List.of(updatedEntity));
    changeEventGenerator.sendChangeNotification(requestContext, changeResult);
    verify(eventProducer, never())
        .send(
            KeyUtil.getKey(updatedEntity),
            EntityChangeEventValue.newBuilder()
                .setUpdateEvent(
                    EntityUpdateEvent.newBuilder()
                        .setPreviousEntity(prevEntities.get(0))
                        .setLatestEntity(updatedEntity)
                        .build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());

    // update correct attributes
    updatedEntity =
        prevEntities.get(0).toBuilder()
            .putAttributes(
                "attribute_key",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value1").build())
                    .build())
            .build();
    changeResult = EntityChangeEvaluator.evaluateChange(prevEntities, List.of(updatedEntity));
    changeEventGenerator.sendChangeNotification(requestContext, changeResult);
    verify(eventProducer, times(1))
        .send(
            KeyUtil.getKey(updatedEntity),
            EntityChangeEventValue.newBuilder()
                .setUpdateEvent(
                    EntityUpdateEvent.newBuilder()
                        .setPreviousEntity(prevEntities.get(0))
                        .setLatestEntity(updatedEntity)
                        .build())
                .setEventTimeMillis(CURRENT_TIME_MILLIS)
                .build());
  }

  private List<Entity> createEntities(int count) {
    return createEntities(count, new HashMap<>());
  }

  private List<Entity> createEntities(int count, Map<String, AttributeValue> attributeValueMap) {
    return IntStream.rangeClosed(1, count)
        .mapToObj(
            i ->
                Entity.newBuilder()
                    .setTenantId(TEST_TENANT_ID)
                    .setEntityType(TEST_ENTITY_TYPE)
                    .setEntityId(UUID.randomUUID().toString())
                    .setEntityName("Test entity " + i)
                    .putAllAttributes(attributeValueMap)
                    .build())
        .collect(Collectors.toList());
  }
}
