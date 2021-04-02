package org.hypertrace.entity.data.service;

import java.util.Comparator;
import java.util.function.BiPredicate;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator;
import org.hypertrace.entity.data.service.v1.Value;

class UpsertConditionMatcher {
  private static final Comparator<AttributeValue> ATTRIBUTE_VALUE_COMPARATOR =
      new AttributeValueComparator();

  boolean matches(Entity existingEntity, UpsertCondition upsertCondition) {
    return !upsertCondition.hasPropertyPredicate()
        || this.matchesPredicate(existingEntity, upsertCondition.getPropertyPredicate());
  }

  private boolean matchesPredicate(Entity entity, Predicate predicate) {
    return this.asBipredicate(predicate.getOperator())
        .test(
            entity
                .getAttributesMap()
                .getOrDefault(predicate.getAttributeKey(), AttributeValue.getDefaultInstance()),
            predicate.getValue());
  }

  private BiPredicate<AttributeValue, AttributeValue> asBipredicate(PredicateOperator operator) {
    switch (operator) {
      case PREDICATE_OPERATOR_EQUALS:
        return AttributeValue::equals;
      case PREDICATE_OPERATOR_NOT_EQUALS:
        return (first, second) -> !first.equals(second);
      case PREDICATE_OPERATOR_GREATER_THAN:
        return (first, second) -> ATTRIBUTE_VALUE_COMPARATOR.compare(first, second) > 0;
      case PREDICATE_OPERATOR_LESS_THAN:
        return (first, second) -> ATTRIBUTE_VALUE_COMPARATOR.compare(first, second) < 0;
      case PREDICATE_OPERATOR_UNSPECIFIED:
      case UNRECOGNIZED:
      default:
        throw new IllegalArgumentException("Unrecognized operator: " + operator);
    }
  }

  private static final class AttributeValueComparator implements Comparator<AttributeValue> {
    @Override
    public int compare(AttributeValue o1, AttributeValue o2) {
      if (o1.getValue().getTypeCase() != o2.getValue().getTypeCase()) {
        throw new IllegalArgumentException(
            String.format(
                "Mismatched values %s and %s",
                o1.getValue().getTypeCase(), o2.getValue().getTypeCase()));
      }

      Comparable<Object> first = this.getComparableValue(o1);
      Comparable<Object> second = this.getComparableValue(o2);
      return first.compareTo(second);
    }

    @SuppressWarnings("unchecked")
    private Comparable<Object> getComparableValue(AttributeValue attributeValue) {
      switch (attributeValue.getTypeCase()) {
        case VALUE:
          return (Comparable<Object>) this.getComparableValue(attributeValue.getValue());
        case VALUE_LIST:
        case VALUE_MAP:
        case TYPE_NOT_SET:
        default:
          throw new IllegalArgumentException(
              "Unsupported value type: " + attributeValue.getTypeCase());
      }
    }

    private Comparable<?> getComparableValue(Value value) {
      switch (value.getTypeCase()) {
        case INT:
          return value.getInt();
        case STRING:
          return value.getString();
        case BOOLEAN:
          return value.getBoolean();
        case LONG:
          return value.getLong();
        case FLOAT:
          return value.getFloat();
        case DOUBLE:
          return value.getDouble();
        case BYTES:
          return value.getBytes().asReadOnlyByteBuffer();
        case TIMESTAMP:
          return value.getTimestamp();
        case CUSTOM:
        case TYPE_NOT_SET:
        default:
          throw new IllegalArgumentException("Unexpected value: " + value.getTypeCase());
      }
    }
  }
}
