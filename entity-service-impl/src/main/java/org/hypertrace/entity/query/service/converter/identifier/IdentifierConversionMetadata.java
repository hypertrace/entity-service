package org.hypertrace.entity.query.service.converter.identifier;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.Value;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ValueType;

@Value
@Builder
public class IdentifierConversionMetadata {
  String subDocPath;

  @Default ValueType valueType = ValueType.UNRECOGNIZED;

  @Default Operator operator = Operator.UNRECOGNIZED;
}
