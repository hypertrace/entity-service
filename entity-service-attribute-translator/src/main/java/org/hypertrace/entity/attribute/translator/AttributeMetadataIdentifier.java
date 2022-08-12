package org.hypertrace.entity.attribute.translator;

import lombok.Value;

@Value
public class AttributeMetadataIdentifier {
  String scope;
  String docStorePath;
}
