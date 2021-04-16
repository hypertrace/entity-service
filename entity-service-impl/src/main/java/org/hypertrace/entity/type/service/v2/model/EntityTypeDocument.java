package org.hypertrace.entity.type.service.v2.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.type.service.v2.EntityType;

public class EntityTypeDocument implements Document {
  // Since there could be additional metadata fields written by the doc store,
  // ignore them while reading the object from JSON.
  private static final ObjectMapper OBJECT_MAPPER =
      new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

  @JsonProperty(value = EntityServiceConstants.TENANT_ID)
  private String tenantId;

  @JsonProperty(value = EntityServiceConstants.NAME)
  private String name;

  @JsonProperty private String attributeScope;

  @JsonProperty private String idAttributeKey;

  @JsonProperty private String nameAttributeKey;

  @JsonProperty private String timestampAttributeKey;

  @JsonProperty private List<String> requiredKeys;

  public EntityTypeDocument() {}

  EntityTypeDocument(
      String tenantId,
      String name,
      String attributeScope,
      String idAttributeKey,
      String nameAttributeKey,
      String timestampAttributeKey,
      List<String> requiredKeys) {
    this.tenantId = tenantId;
    this.name = name;
    this.attributeScope = attributeScope;
    this.idAttributeKey = idAttributeKey;
    this.nameAttributeKey = nameAttributeKey;
    this.timestampAttributeKey = timestampAttributeKey;
    this.requiredKeys = requiredKeys;
  }

  public static EntityTypeDocument fromProto(@Nonnull String tenantId, EntityType entityType) {
    return new EntityTypeDocument(
        tenantId,
        entityType.getName(),
        entityType.getAttributeScope(),
        entityType.getIdAttributeKey(),
        entityType.getNameAttributeKey(),
        entityType.getTimestampAttributeKey(),
        entityType.getRequiredKeysList());
  }

  public EntityType toProto() {
    EntityType.Builder builder =
        EntityType.newBuilder()
            .setName(getName())
            .setAttributeScope(getAttributeScope())
            .setIdAttributeKey(getIdAttributeKey())
            .addAllRequiredKeys(getRequiredKeys());

    getNameAttributeKey().ifPresent(builder::setNameAttributeKey);
    getTimestampAttributeKey().ifPresent(builder::setTimestampAttributeKey);
    return builder.build();
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

  public String getIdAttributeKey() {
    return idAttributeKey;
  }

  @JsonIgnore
  public Optional<String> getNameAttributeKey() {
    return Optional.of(nameAttributeKey);
  }

  @JsonIgnore
  public Optional<String> getTimestampAttributeKey() {
    return Optional.ofNullable(timestampAttributeKey);
  }

  public List<String> getRequiredKeys() {
    return Optional.ofNullable(requiredKeys).orElse(Collections.emptyList());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EntityTypeDocument that = (EntityTypeDocument) o;
    return Objects.equals(getTenantId(), that.getTenantId())
        && Objects.equals(getName(), that.getName())
        && Objects.equals(getAttributeScope(), that.getAttributeScope())
        && Objects.equals(getIdAttributeKey(), that.getIdAttributeKey())
        && Objects.equals(getNameAttributeKey(), that.getNameAttributeKey())
        && Objects.equals(getTimestampAttributeKey(), that.getTimestampAttributeKey())
        && Objects.equals(getRequiredKeys(), that.getRequiredKeys());
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getTenantId(),
        getName(),
        getAttributeScope(),
        getIdAttributeKey(),
        getNameAttributeKey(),
        getTimestampAttributeKey(),
        getRequiredKeys());
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
