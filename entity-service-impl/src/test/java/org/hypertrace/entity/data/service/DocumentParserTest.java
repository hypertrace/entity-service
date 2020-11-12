package org.hypertrace.entity.data.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.Optional;
import java.util.UUID;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.junit.jupiter.api.Test;

class DocumentParserTest {
  private final Entity ENTITY =
      Entity.newBuilder()
          .setTenantId("tenant-id-1")
          .setEntityType("ENTITY_TYPE")
          .setEntityId(UUID.randomUUID().toString())
          .setEntityName("Test entity")
          .putAttributes(
              "foo",
              AttributeValue.newBuilder().setValue(Value.newBuilder().setString("foo1")).build())
          .build();

  private final JSONDocument GOOD_JSON = new JSONDocument(JsonFormat.printer().print(ENTITY));
  // parser is very forgiving - give it a completely different data type to prevent it from
  // defaulting
  private final JSONDocument BAD_JSON = new JSONDocument("{\"entityId\": [1, 2]}");
  private final DocumentParser PARSER = new DocumentParser();

  DocumentParserTest() throws IOException {}

  @Test
  void throwIfFailedParseOrThrow() {
    assertThrows(
        InvalidProtocolBufferException.class,
        () -> PARSER.parseOrThrow(BAD_JSON, Entity.newBuilder()));
  }

  @Test
  void returnsMessageIfAbleToParseOrThrow() throws InvalidProtocolBufferException {
    assertEquals(ENTITY, PARSER.parseOrThrow(GOOD_JSON, Entity.newBuilder()));
  }

  @Test
  void returnsMessageOptionalIfAbleToParseOrLog() {
    assertEquals(Optional.of(ENTITY), PARSER.parseOrLog(GOOD_JSON, Entity.newBuilder()));
  }

  @Test
  void returnsEmptyOptionalIfFailedParseOrLog() {
    assertEquals(Optional.empty(), PARSER.parseOrLog(BAD_JSON, Entity.newBuilder()));
  }
}
