package org.hypertrace.entity.data.service;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EntityRelationshipDocKeyTest {

  @Test
  void testEqualsHashcode() {
    EntityRelationshipDocKey tenant1Pod1Container1 = new EntityRelationshipDocKey(
        "t1", "POD_CONTAINER", "pod1", "container1");
    EntityRelationshipDocKey tenant1Pod1Container2 = new EntityRelationshipDocKey(
        "t1", "POD_CONTAINER", "pod1", "container2");
    EntityRelationshipDocKey tenant1Pod2Container1 = new EntityRelationshipDocKey(
        "t1", "POD_CONTAINER", "pod2", "container1");
    EntityRelationshipDocKey tenant2Pod1Container1 = new EntityRelationshipDocKey(
        "t2", "POD_CONTAINER", "pod1", "container1");

    EntityRelationshipDocKey tenantPod1Container1Duplicate = new EntityRelationshipDocKey(
        "t1", "POD_CONTAINER", "pod1", "container1");

    Assertions.assertNotEquals(tenant1Pod1Container1, tenant1Pod1Container2);
    Assertions.assertNotEquals(tenant1Pod1Container1, tenant1Pod2Container1);
    Assertions.assertNotEquals(tenant1Pod1Container1, tenant2Pod1Container1);

    Assertions.assertEquals(tenant1Pod1Container1, tenantPod1Container1Duplicate);
    Assertions
        .assertEquals(tenant1Pod1Container1.hashCode(), tenantPod1Container1Duplicate.hashCode());
  }
}
