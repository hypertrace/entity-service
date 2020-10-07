package org.hypertrace.entity.query.service.client;

import java.util.Objects;

public class EntityTypeAndId {
  private final String type;
  private final String id;
  private final String idColumnName;
  private final String labelsColumnName;

  public EntityTypeAndId(String type, String id, String idColumnName, String labelsColumnName) {
    this.type = type;
    this.id = id;
    this.idColumnName = idColumnName;
    this.labelsColumnName = labelsColumnName;
  }

  public String getType() {
    return type;
  }

  public String getId() {
    return id;
  }

  public String getIdColumnName() {
    return idColumnName;
  }

  public String getLabelsColumnName() {
    return labelsColumnName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EntityTypeAndId that = (EntityTypeAndId) o;
    return Objects.equals(type, that.type) &&
        Objects.equals(id, that.id) &&
        Objects.equals(idColumnName, that.idColumnName) &&
        Objects.equals(labelsColumnName, that.labelsColumnName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, id, idColumnName, labelsColumnName);
  }
}
