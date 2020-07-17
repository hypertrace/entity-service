package org.hypertrace.entity.data.service.client;

import java.util.Objects;
import org.hypertrace.entity.data.service.v1.ByTypeAndIdentifyingAttributes;

public class EdsTypeAndIdAttributesCacheKey {

  public final String tenantId;
  public final ByTypeAndIdentifyingAttributes byTypeAndIdentifyingAttributes;

  public EdsTypeAndIdAttributesCacheKey(String tenantId, ByTypeAndIdentifyingAttributes
      byTypeAndIdentifyingAttributes) {
    this.tenantId = tenantId;
    this.byTypeAndIdentifyingAttributes = byTypeAndIdentifyingAttributes;
  }

  @Override
  public String toString() {
    return String.format("__%s__%s", this.tenantId, this.byTypeAndIdentifyingAttributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, byTypeAndIdentifyingAttributes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EdsTypeAndIdAttributesCacheKey that = (EdsTypeAndIdAttributesCacheKey) o;
    return Objects.equals(tenantId, that.tenantId)
        && Objects.equals(byTypeAndIdentifyingAttributes, that.byTypeAndIdentifyingAttributes);
  }

}
