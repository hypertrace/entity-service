package org.hypertrace.entity.data.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Single;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hypertrace.entity.data.service.EntityDataServiceImpl.ErrorMessages;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.type.service.rxclient.EntityTypeClient;
import org.hypertrace.entity.type.service.v1.AttributeKind;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityNormalizerTest {
  private static final String TENANT_ID = "tenant";
  private static final String V1_ENTITY_TYPE = "v1-entity";
  private static final String V2_ENTITY_TYPE = "v2-entity";
  private static final AttributeType V1_ID_ATTR =
      AttributeType.newBuilder()
          .setIdentifyingAttribute(true)
          .setValueKind(AttributeKind.TYPE_STRING)
          .setName("required-attr")
          .build();
  @Mock EntityTypeClient mockEntityTypeClient;
  @Mock EntityIdGenerator mockIdGenerator;
  @Mock IdentifyingAttributeCache mockIdAttrCache;
  private EntityNormalizer normalizer;

  @BeforeEach
  void beforeEach() {
    this.normalizer = new EntityNormalizer(mockEntityTypeClient, mockIdGenerator, mockIdAttrCache);
  }

  @Test
  void throwsOnMissingEntityType() {
    Exception exception =
        assertThrows(
            RuntimeException.class,
            () -> this.normalizer.normalize(TENANT_ID, Entity.getDefaultInstance()));
    assertEquals(exception.getMessage(), ErrorMessages.ENTITY_TYPE_EMPTY);
  }

  @Test
  void throwsOnV1EntityTypeMissingIdAttr() {
    when(this.mockIdAttrCache.getIdentifyingAttributes(TENANT_ID, V1_ENTITY_TYPE))
        .thenReturn(List.of(V1_ID_ATTR));
    when(this.mockEntityTypeClient.get(V1_ENTITY_TYPE))
        .thenReturn(Single.error(new RuntimeException()));
    Entity inputEntity = Entity.newBuilder().setEntityType(V1_ENTITY_TYPE).build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> this.normalizer.normalize(TENANT_ID, inputEntity));
    assertEquals(
        "Received and expected identifying attributes differ. Received: [] . Expected: [required-attr]",
        exception.getMessage());
  }

  @Test
  void throwsOnV1EntityTypeWithExtraIdAttr() {
    Map<String, AttributeValue> valueMap =
        buildValueMap(Map.of(V1_ID_ATTR.getName(), "foo-value", "other", "other-value"));
    when(this.mockIdAttrCache.getIdentifyingAttributes(TENANT_ID, V1_ENTITY_TYPE))
        .thenReturn(List.of(V1_ID_ATTR));
    when(this.mockEntityTypeClient.get(V1_ENTITY_TYPE))
        .thenReturn(Single.error(new RuntimeException()));
    Entity inputEntity =
        Entity.newBuilder()
            .setEntityType(V1_ENTITY_TYPE)
            .putAllIdentifyingAttributes(valueMap)
            .build();

    Exception exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> this.normalizer.normalize(TENANT_ID, inputEntity));
    assertEquals(
        "Received and expected identifying attributes differ. Received: [other, required-attr] . Expected: [required-attr]",
        exception.getMessage());
  }

  @Test
  void throwsOnV2EntityMissingId() {
    when(this.mockEntityTypeClient.get(V2_ENTITY_TYPE))
        .thenReturn(Single.just(EntityType.getDefaultInstance()));
    Entity inputEntity = Entity.newBuilder().setEntityType(V2_ENTITY_TYPE).build();

    Exception exception =
        assertThrows(
            RuntimeException.class, () -> this.normalizer.normalize(TENANT_ID, inputEntity));
    assertEquals(
        exception.getMessage(), "No identifying attributes defined for EntityType: v2-entity");
  }

  @Test
  void normalizesV1EntityWithAttrs() {
    Map<String, AttributeValue> valueMap = buildValueMap(Map.of(V1_ID_ATTR.getName(), "foo-value"));
    when(this.mockIdGenerator.generateEntityId(TENANT_ID, V1_ENTITY_TYPE, valueMap))
        .thenReturn("generated-id");
    when(this.mockIdAttrCache.getIdentifyingAttributes(TENANT_ID, V1_ENTITY_TYPE))
        .thenReturn(List.of(V1_ID_ATTR));
    when(this.mockEntityTypeClient.get(V1_ENTITY_TYPE))
        .thenReturn(Single.error(new RuntimeException()));
    Entity inputEntity =
        Entity.newBuilder()
            .setEntityType(V1_ENTITY_TYPE)
            .putAllIdentifyingAttributes(valueMap)
            .build();

    Entity expectedNormalized =
        inputEntity.toBuilder()
            .setEntityId("generated-id")
            .setTenantId(TENANT_ID)
            .putAllAttributes(valueMap)
            .build();
    assertEquals(expectedNormalized, this.normalizer.normalize(TENANT_ID, inputEntity));
  }

  @Test
  void normalizesV2EntityWithId() {
    when(this.mockEntityTypeClient.get(V2_ENTITY_TYPE))
        .thenReturn(Single.just(EntityType.getDefaultInstance()));
    Entity inputEntity =
        Entity.newBuilder().setEntityType(V2_ENTITY_TYPE).setEntityId("input-id").build();

    Entity expectedNormalized = inputEntity.toBuilder().setTenantId(TENANT_ID).build();
    assertEquals(expectedNormalized, this.normalizer.normalize(TENANT_ID, inputEntity));
  }

  private Map<String, AttributeValue> buildValueMap(Map<String, String> stringMap) {
    return stringMap.entrySet().stream()
        .map(
            entry ->
                Map.entry(
                    entry.getKey(),
                    AttributeValue.newBuilder()
                        .setValue(Value.newBuilder().setString(entry.getValue()))
                        .build()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }
}
