package org.hypertrace.entity.data.service.rxclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityKeyTest {
  private static final String TENANT_ID_1 = "tenant id 1";
  private static final String TENANT_ID_2 = "tenant id 2";
  private static final RequestContext REQUEST_CONTEXT_1 = RequestContext.forTenantId(TENANT_ID_1);
  private static final RequestContext REQUEST_CONTEXT_2 = RequestContext.forTenantId(TENANT_ID_2);

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
  void matchesSameEntityIdSameRequestContext() {
    RequestContext matchingContext = RequestContext.forTenantId(TENANT_ID_1);

    // Change some attributes to make sure it's not a deep compare
    Entity matchingEntity = this.entityV2.toBuilder().clearAttributes().build();

    assertEquals(
        new EntityKey(REQUEST_CONTEXT_1, this.entityV2),
        new EntityKey(matchingContext, matchingEntity));
  }

  @Test
  void doesNotMatchSameEntityDifferentRequestContext() {
    assertNotEquals(
        new EntityKey(REQUEST_CONTEXT_1, this.entityV2),
        new EntityKey(REQUEST_CONTEXT_2, this.entityV2));
  }

  @Test
  void doesNotMatchDifferentEntitySameRequestContext() {
    Entity differentEntity = this.entityV2.toBuilder().setEntityId("id-2").build();

    assertNotEquals(
        new EntityKey(REQUEST_CONTEXT_1, this.entityV2),
        new EntityKey(REQUEST_CONTEXT_2, differentEntity));
  }

  @Test
  void matchesSameIdentifyingAttributesSameRequestContext() {
    RequestContext matchingContext = RequestContext.forTenantId(TENANT_ID_1);

    // Change some attributes to make sure it's not a deep compare
    Entity matchingEntity = this.entityV1.toBuilder().clearAttributes().build();

    assertEquals(
        new EntityKey(REQUEST_CONTEXT_1, this.entityV1),
        new EntityKey(matchingContext, matchingEntity));
  }

  @Test
  void doesNotMatchDifferentIdentifyingAttributesSameRequestContext() {
    Entity diffEntity =
        this.entityV1.toBuilder().putIdentifyingAttributes("key-2", buildValue("value-2")).build();

    assertNotEquals(
        new EntityKey(REQUEST_CONTEXT_1, this.entityV1),
        new EntityKey(REQUEST_CONTEXT_1, diffEntity));
  }

  @Test
  void doesNotMatchSameIdentifyingAttributesDifferentRequestContext() {
    assertNotEquals(
        new EntityKey(REQUEST_CONTEXT_1, this.entityV1),
        new EntityKey(REQUEST_CONTEXT_2, this.entityV1));
  }

  private static AttributeValue buildValue(String value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setString(value)).build();
  }
}
