package org.hypertrace.entity.type.service.v2.model;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.util.List;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.entity.type.service.v2.EntityType.EntityFormationCondition;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link EntityTypeDocument} */
public class EntityTypeDocumentTest {
  @Test
  public void testProtoConversion() {
    EntityType entityType =
        EntityType.newBuilder()
            .setName("API")
            .setAttributeScope("API")
            .setIdAttributeKey("id")
            .setNameAttributeKey("name")
            .setTimestampAttributeKey("timestamp")
            .addRequiredConditions(EntityFormationCondition.newBuilder().setRequiredKey("other"))
            .build();
    Assertions.assertEquals(
        entityType, EntityTypeDocument.fromProto("testTenant", entityType).toProto());
  }

  @Test
  public void testJsonConversion() throws JsonProcessingException {
    EntityTypeDocument document =
        new EntityTypeDocument(
            "testTenant",
            "API",
            "API",
            "id",
            "name",
            "timestamp",
            List.of(EntityFormationCondition.newBuilder().setRequiredKey("other").build()));
    Assertions.assertEquals(document, EntityTypeDocument.fromJson(document.toJson()));
  }

  @Test
  public void testFromJsonMissingField()
      throws InvalidProtocolBufferException, JsonProcessingException {
    EntityType entityType =
        EntityType.newBuilder()
            .setName("API")
            .setAttributeScope("API")
            .setIdAttributeKey("id")
            .setNameAttributeKey("name")
            .build();
    String entityTypeJson = JsonFormat.printer().print(entityType);
    Assertions.assertEquals(entityType, EntityTypeDocument.fromJson(entityTypeJson).toProto());
  }

  @Test
  public void testToProtoMissingField() {
    EntityTypeDocument document =
        new EntityTypeDocument("testTenant", "API", "API", "id", "name", null, null);
    Assertions.assertEquals(
        EntityType.newBuilder()
            .setName("API")
            .setAttributeScope("API")
            .setIdAttributeKey("id")
            .setNameAttributeKey("name")
            .build(),
        document.toProto());
  }
}
