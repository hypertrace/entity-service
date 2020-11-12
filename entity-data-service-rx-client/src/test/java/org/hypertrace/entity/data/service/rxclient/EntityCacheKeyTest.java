package org.hypertrace.entity.data.service.rxclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityCacheKeyTest {

  @Mock RequestContext mockRequestContext;

  final Entity entityV2 =
      Entity.newBuilder()
          .setEntityId("id-1")
          .setEntityType("type-v2")
          .setEntityName("name-1")
          .putAttributes("key", buildValue("value"))
          .build();

  final Entity entityV1 =
      Entity.newBuilder()
          .setEntityType("type-v1")
          .setEntityName("name-1")
          .putIdentifyingAttributes("key", buildValue("value"))
          .putAttributes("other-key", buildValue("other-alue"))
          .build();

  @Test
  void matchesSameEntityIdSameTenant() {
    RequestContext matchingContext = mock(RequestContext.class);
    when(this.mockRequestContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));
    when(matchingContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));

    // Change some attributes to make sure it's not a deep compare
    Entity matchingEntity = this.entityV2.toBuilder().clearAttributes().build();

    assertEquals(
        new EntityCacheKey(this.mockRequestContext, this.entityV2),
        new EntityCacheKey(matchingContext, matchingEntity));
  }

  @Test
  void doesNotMatchSameEntityDifferentTenant() {
    RequestContext differentContext = mock(RequestContext.class);
    when(this.mockRequestContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));
    when(differentContext.getTenantId()).thenReturn(Optional.of("tenant id 2"));
    assertNotEquals(
        new EntityCacheKey(this.mockRequestContext, this.entityV2),
        new EntityCacheKey(differentContext, this.entityV2));
  }

  @Test
  void doesNotMatchDifferentEntitySameTenant() {
    when(this.mockRequestContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));
    Entity differentEntity = this.entityV2.toBuilder().setEntityId("id-2").build();

    assertNotEquals(
        new EntityCacheKey(this.mockRequestContext, this.entityV2),
        new EntityCacheKey(this.mockRequestContext, differentEntity));
  }

  @Test
  void matchesSameIdentifyingAttributesSameTenant() {
    RequestContext matchingContext = mock(RequestContext.class);
    when(this.mockRequestContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));
    when(matchingContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));

    // Change some attributes to make sure it's not a deep compare
    Entity matchingEntity = this.entityV1.toBuilder().clearAttributes().build();

    assertEquals(
        new EntityCacheKey(this.mockRequestContext, this.entityV1),
        new EntityCacheKey(matchingContext, matchingEntity));
  }

  @Test
  void doesNotMatchDifferentIdentifyingAttributesSameTenant() {
    when(this.mockRequestContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));

    Entity diffEntity =
        this.entityV1.toBuilder().putIdentifyingAttributes("key-2", buildValue("value-2")).build();

    assertNotEquals(
        new EntityCacheKey(this.mockRequestContext, this.entityV1),
        new EntityCacheKey(this.mockRequestContext, diffEntity));
  }

  @Test
  void doesNotMatchSameIdentifyingAttributesDifferentTenant() {
    RequestContext otherContext = mock(RequestContext.class);
    when(this.mockRequestContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));
    when(otherContext.getTenantId()).thenReturn(Optional.of("tenant id 2"));

    assertNotEquals(
        new EntityCacheKey(this.mockRequestContext, this.entityV1),
        new EntityCacheKey(otherContext, this.entityV1));
  }

  private static AttributeValue buildValue(String value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setString(value)).build();
  }
}
