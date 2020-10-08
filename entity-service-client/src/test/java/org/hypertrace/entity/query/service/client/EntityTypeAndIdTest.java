package org.hypertrace.entity.query.service.client;

import java.util.Objects;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EntityTypeAndIdTest {
  @Test
  public void testEntityTypeAndId() {
    EntityTypeAndId entityTypeAndId1 = new EntityTypeAndId("type1", "id1",
        "id", "labels");
    EntityTypeAndId entityTypeAndId2 = new EntityTypeAndId("type1", "id2",
        "id", "labels");
    EntityTypeAndId entityTypeAndId3 = new EntityTypeAndId("type1", "id1",
        "id_", "labels");
    EntityTypeAndId entityTypeAndId4 = new EntityTypeAndId("type1", "id1",
        "id", "labels_");
    EntityTypeAndId entityTypeAndId5 = new EntityTypeAndId("type1", "id1",
        "id", "labels");
    EntityTypeAndId entityTypeAndId6 = new EntityTypeAndId("type2", "id1",
        "id", "labels");

    Assertions.assertEquals("type1", entityTypeAndId1.getType());
    Assertions.assertEquals("id1", entityTypeAndId1.getId());
    Assertions.assertEquals("id", entityTypeAndId1.getIdColumnName());
    Assertions.assertEquals("labels", entityTypeAndId1.getLabelsColumnName());

    Assertions.assertEquals(entityTypeAndId1, entityTypeAndId1);
    Assertions.assertNotEquals(entityTypeAndId1, entityTypeAndId2);
    Assertions.assertNotEquals(entityTypeAndId1, entityTypeAndId3);
    Assertions.assertNotEquals(entityTypeAndId1, entityTypeAndId4);
    Assertions.assertEquals(entityTypeAndId1, entityTypeAndId5);
    Assertions.assertNotEquals(entityTypeAndId1, entityTypeAndId6);
    Assertions.assertNotEquals(entityTypeAndId1, null);
    Assertions.assertNotEquals(entityTypeAndId1, 7);

    Assertions.assertEquals(Objects.hashCode(entityTypeAndId1), Objects.hashCode(entityTypeAndId5));
    Assertions.assertNotEquals(Objects.hashCode(entityTypeAndId1), Objects.hashCode(entityTypeAndId2));
  }
}
