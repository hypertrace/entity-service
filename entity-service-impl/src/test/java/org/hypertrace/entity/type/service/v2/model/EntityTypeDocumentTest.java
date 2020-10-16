package org.hypertrace.entity.type.service.v2.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link EntityTypeDocument}
 */
public class EntityTypeDocumentTest {
  @Test
  public void testProtoConversion() {
    EntityType entityType = EntityType.newBuilder().setName("API").setAttributeScope("API")
        .setIdAttributeKey("id").setNameAttributeKey("name").build();
    Assertions.assertEquals(entityType, EntityTypeDocument.fromProto("testTenant", entityType).toProto());
  }

  @Test
  public void testJsonConversion() throws JsonProcessingException {
    EntityTypeDocument document =
        new EntityTypeDocument("testTenant", "API", "API", "id", "name");
    Assertions.assertEquals(document, EntityTypeDocument.fromJson(document.toJson()));
  }
}
