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

  Entity entity =
      Entity.newBuilder()
          .setEntityId("id-1")
          .setEntityType("type-1")
          .setEntityName("name-1")
          .putAttributes(
              "key",
              AttributeValue.newBuilder().setValue(Value.newBuilder().setBoolean(true)).build())
          .build();

  @Test
  void matchesSameEntityIdSameTenant() {
    RequestContext matchingContext = mock(RequestContext.class);
    when(this.mockRequestContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));
    when(matchingContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));

    // Change some attributes to make sure it's not a deep compare
    Entity matchingEntity = this.entity.toBuilder().clearAttributes().build();

    assertEquals(
        new EntityCacheKey(this.mockRequestContext, this.entity),
        new EntityCacheKey(matchingContext, matchingEntity));
  }

  @Test
  void doesNotMatchSameEntityDifferentTenant() {
    RequestContext differentContext = mock(RequestContext.class);
    when(this.mockRequestContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));
    when(differentContext.getTenantId()).thenReturn(Optional.of("tenant id 2"));
    assertNotEquals(
        new EntityCacheKey(this.mockRequestContext, this.entity),
        new EntityCacheKey(differentContext, this.entity));
  }

  @Test
  void doesNotMatchDifferentEntitySameTenant() {
    when(this.mockRequestContext.getTenantId()).thenReturn(Optional.of("tenant id 1"));
    Entity differentEntity = this.entity.toBuilder().setEntityId("id-2").build();

    assertNotEquals(
        new EntityCacheKey(this.mockRequestContext, this.entity),
        new EntityCacheKey(this.mockRequestContext, differentEntity));
  }
}
