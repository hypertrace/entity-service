package org.hypertrace.entity.data.service;

import com.google.common.base.Splitter;
import java.util.Iterator;
import java.util.Objects;

/**
 * Key used to identify an EntityRelationship in the Document store
 */
public class EntityRelationshipDocKey implements org.hypertrace.core.documentstore.Key {

  private static final String SEPARATOR = ":";
  private static final Splitter SPLITTER = Splitter.on(SEPARATOR);

  private final String tenantId;
  private final String relationshipType;
  private final String fromEntityId;
  private final String toEntityId;

  public EntityRelationshipDocKey(String tenantId, String type, String fromEntityId,
      String toEntityId) {
    this.tenantId = tenantId;
    this.relationshipType = type;
    this.fromEntityId = fromEntityId;
    this.toEntityId = toEntityId;
  }

  public static EntityRelationshipDocKey parseFrom(String idStr) {
    if (idStr == null) {
      return null;
    }

    Iterator<String> iterator = SPLITTER.split(idStr).iterator();

    String tenantId = null, type = null, fromId = null, toId = null;
    if (iterator.hasNext()) {
      tenantId = iterator.next();
    }
    if (iterator.hasNext()) {
      type = iterator.next();
    }
    if (iterator.hasNext()) {
      fromId = iterator.next();
    }
    if (iterator.hasNext()) {
      toId = iterator.next();
    }

    if (type != null && fromId != null && toId != null) {
      return new EntityRelationshipDocKey(tenantId, type, fromId, toId);
    }

    return null;
  }

  @Override
  public String toString() {
    return tenantId + SEPARATOR + relationshipType + SEPARATOR + fromEntityId + SEPARATOR
        + toEntityId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EntityRelationshipDocKey that = (EntityRelationshipDocKey) o;
    return Objects.equals(tenantId, that.tenantId) &&
        Objects.equals(relationshipType, that.relationshipType) &&
        Objects.equals(fromEntityId, that.fromEntityId) &&
        Objects.equals(toEntityId, that.toEntityId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, relationshipType, fromEntityId, toEntityId);
  }
}

