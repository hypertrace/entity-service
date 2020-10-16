package org.hypertrace.entity.type.service.v2.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.type.service.v2.EntityType;

public class EntityTypeDocument implements Document {
  // Since there could be additional metadata fields written by the doc store,
  // ignore them while reading the object from JSON.
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper().configure(
      DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = EntityServiceConstants.TENANT_ID)
  private String tenantId;

  @JsonProperty(value = EntityServiceConstants.NAME)
  private String name;

  @JsonProperty
  private String attributeScope;

  @JsonProperty
  private String idAttributeKey;

  @JsonProperty
  private String nameAttributeKey;

  public EntityTypeDocument() {}

  public EntityTypeDocument(String tenantId, String name, String attributeScope, String idAttributeKey, String nameAttributeKey) {
    this.tenantId = tenantId;
    this.name = name;
    this.attributeScope = attributeScope;
    this.idAttributeKey = idAttributeKey;
    this.nameAttributeKey = nameAttributeKey;
  }

  public static EntityTypeDocument fromProto(@Nonnull String tenantId, EntityType entityType) {
    return new EntityTypeDocument(tenantId, entityType.getName(), entityType.getAttributeScope(),
        entityType.getIdAttributeKey(), entityType.getNameAttributeKey());
  }

  public EntityType toProto() {
    return EntityType.newBuilder().setName(getName()).setAttributeScope(getAttributeScope())
        .setIdAttributeKey(getIdAttributeKey()).setNameAttributeKey(getNameAttributeKey()).build();
  }

  public static EntityTypeDocument fromJson(String json) throws JsonProcessingException {
    return OBJECT_MAPPER.readValue(json, EntityTypeDocument.class);
  }

  public String getTenantId() {
    return tenantId;
  }

  public void setTenantId(String tenantId) {
    this.tenantId = tenantId;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getAttributeScope() {
    return attributeScope;
  }

  public void setAttributeScope(String attributeScope) {
    this.attributeScope = attributeScope;
  }

  public String getIdAttributeKey() {
    return idAttributeKey;
  }

  public void setIdAttributeKey(String idAttributeKey) {
    this.idAttributeKey = idAttributeKey;
  }

  public String getNameAttributeKey() {
    return nameAttributeKey;
  }

  public void setNameAttributeKey(String nameAttributeKey) {
    this.nameAttributeKey = nameAttributeKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EntityTypeDocument document = (EntityTypeDocument) o;
    return Objects.equals(tenantId, document.tenantId) &&
        Objects.equals(name, document.name) &&
        Objects.equals(attributeScope, document.attributeScope) &&
        Objects.equals(idAttributeKey, document.idAttributeKey) &&
        Objects.equals(nameAttributeKey, document.nameAttributeKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, name, attributeScope, idAttributeKey, nameAttributeKey);
  }

  @Override
  public String toJson() {
    try {
      return OBJECT_MAPPER.writeValueAsString(this);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
