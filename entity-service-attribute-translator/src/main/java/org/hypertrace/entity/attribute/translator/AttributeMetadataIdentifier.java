package org.hypertrace.entity.attribute.translator;

import lombok.Value;

@Value
public class AttributeMetadataIdentifier {
  String scope;
  String docStorePath;

  public AttributeMetadataIdentifier(String scope, String docStorePath) {
    this.docStorePath = docStorePath;
    this.scope = scope;
  }
}
