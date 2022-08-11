package org.hypertrace.entity.attribute.translator;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class AttributeMetadata {
  private String scope;
  private String docStorePath;

  public AttributeMetadata(String scope, String docStorePath) {
    this.docStorePath = docStorePath;
    this.scope = scope;
  }
}
