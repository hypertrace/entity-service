package org.hypertrace.entity.data.service;

import static org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator.PREDICATE_OPERATOR_EQUALS;
import static org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator.PREDICATE_OPERATOR_GREATER_THAN;
import static org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator.PREDICATE_OPERATOR_LESS_THAN;
import static org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator.PREDICATE_OPERATOR_NOT_EQUALS;
import static org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator.PREDICATE_OPERATOR_UNSPECIFIED;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.AttributeValueList;
import org.hypertrace.entity.data.service.v1.AttributeValueMap;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition.Predicate.PredicateOperator;
import org.hypertrace.entity.data.service.v1.Value;
import org.junit.jupiter.api.Test;

class UpsertConditionMatcherTest {

  private final UpsertConditionMatcher matcher = new UpsertConditionMatcher();

  @Test
  void matchesEquality() {
    verifyOperatorBehavior(string("foo"), string("foo"), string("bar"), PREDICATE_OPERATOR_EQUALS);
    verifyOperatorBehavior(
        stringList("foo1", "foo2"),
        stringList("foo1", "foo2"),
        stringList("foo1", "bar2"),
        PREDICATE_OPERATOR_EQUALS);
    verifyOperatorBehavior(
        stringMap(Map.of("k1", "v1", "k2", "v2")),
        stringMap(Map.of("k1", "v1", "k2", "v2")),
        stringMap(Map.of("k1", "v1", "k2", "v2_other")),
        PREDICATE_OPERATOR_EQUALS);
    verifyOperatorBehavior(
        integerValue(1), integerValue(1), integerValue(2), PREDICATE_OPERATOR_EQUALS);
    verifyOperatorBehavior(longValue(1), longValue(1), longValue(2), PREDICATE_OPERATOR_EQUALS);
    verifyOperatorBehavior(timestamp(1), timestamp(1), timestamp(2), PREDICATE_OPERATOR_EQUALS);
    verifyOperatorBehavior(
        byteValue("foo"), byteValue("foo"), byteValue("bar"), PREDICATE_OPERATOR_EQUALS);
    verifyOperatorBehavior(
        floatValue(3.14f), floatValue(3.14f), floatValue(3.5f), PREDICATE_OPERATOR_EQUALS);
    verifyOperatorBehavior(
        doubleValue(3.14), doubleValue(3.14), doubleValue(3.5), PREDICATE_OPERATOR_EQUALS);
    verifyOperatorBehavior(
        booleanValue(true), booleanValue(true), booleanValue(false), PREDICATE_OPERATOR_EQUALS);
  }

  @Test
  void matchesInequality() {
    verifyOperatorBehavior(
        string("foo"), string("bar"), string("foo"), PREDICATE_OPERATOR_NOT_EQUALS);
    verifyOperatorBehavior(
        stringList("foo1", "foo2"),
        stringList("foo1", "bar2"),
        stringList("foo1", "foo2"),
        PREDICATE_OPERATOR_NOT_EQUALS);
    verifyOperatorBehavior(
        stringMap(Map.of("k1", "v1", "k2", "v2")),
        stringMap(Map.of("k1", "v1", "k2", "v2_other")),
        stringMap(Map.of("k1", "v1", "k2", "v2")),
        PREDICATE_OPERATOR_NOT_EQUALS);
    verifyOperatorBehavior(
        integerValue(2), integerValue(1), integerValue(2), PREDICATE_OPERATOR_NOT_EQUALS);
    verifyOperatorBehavior(longValue(2), longValue(1), longValue(2), PREDICATE_OPERATOR_NOT_EQUALS);
    verifyOperatorBehavior(timestamp(2), timestamp(1), timestamp(2), PREDICATE_OPERATOR_NOT_EQUALS);
    verifyOperatorBehavior(
        byteValue("bar"), byteValue("foo"), byteValue("bar"), PREDICATE_OPERATOR_NOT_EQUALS);
    verifyOperatorBehavior(
        floatValue(3.5f), floatValue(3.14f), floatValue(3.5f), PREDICATE_OPERATOR_NOT_EQUALS);
    verifyOperatorBehavior(
        doubleValue(3.5f), doubleValue(3.14), doubleValue(3.5), PREDICATE_OPERATOR_NOT_EQUALS);
    verifyOperatorBehavior(
        booleanValue(false),
        booleanValue(true),
        booleanValue(false),
        PREDICATE_OPERATOR_NOT_EQUALS);
  }

  @Test
  void matchesLessThan() {
    verifyOperatorBehavior(
        integerValue(2), integerValue(3), integerValue(1), PREDICATE_OPERATOR_LESS_THAN);
    verifyOperatorBehavior(longValue(2), longValue(3), longValue(1), PREDICATE_OPERATOR_LESS_THAN);
    verifyOperatorBehavior(timestamp(2), timestamp(3), timestamp(1), PREDICATE_OPERATOR_LESS_THAN);
    verifyOperatorBehavior(
        floatValue(3.5f), floatValue(3.64f), floatValue(3.14f), PREDICATE_OPERATOR_LESS_THAN);
    verifyOperatorBehavior(
        doubleValue(3.5f), doubleValue(3.64), doubleValue(3.14), PREDICATE_OPERATOR_LESS_THAN);
  }

  @Test
  void matchesGreaterThan() {
    verifyOperatorBehavior(
        integerValue(2), integerValue(1), integerValue(3), PREDICATE_OPERATOR_GREATER_THAN);
    verifyOperatorBehavior(
        longValue(2), longValue(1), longValue(3), PREDICATE_OPERATOR_GREATER_THAN);
    verifyOperatorBehavior(
        timestamp(2), timestamp(1), timestamp(3), PREDICATE_OPERATOR_GREATER_THAN);
    verifyOperatorBehavior(
        floatValue(3.5f), floatValue(3.14f), floatValue(3.64f), PREDICATE_OPERATOR_GREATER_THAN);
    verifyOperatorBehavior(
        doubleValue(3.5f), doubleValue(3.14), doubleValue(3.64), PREDICATE_OPERATOR_GREATER_THAN);
  }

  @Test
  void returnsTrueIfNoCondition() {
    assertTrue(matcher.matches(Entity.getDefaultInstance(), UpsertCondition.getDefaultInstance()));
  }

  @Test
  void handlesMissingValues() {
    Entity entity = entity(Map.of());
    assertFalse(
        matcher.matches(entity, condition("key1", PREDICATE_OPERATOR_EQUALS, string("foo"))));
    assertTrue(
        matcher.matches(entity, condition("key1", PREDICATE_OPERATOR_NOT_EQUALS, string("foo"))));

    // Throws for comparisons
    assertThrows(
        IllegalArgumentException.class,
        () ->
            matcher.matches(
                entity, condition("key1", PREDICATE_OPERATOR_LESS_THAN, integerValue(10))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            matcher.matches(
                entity, condition("key1", PREDICATE_OPERATOR_GREATER_THAN, integerValue(10))));
  }

  @Test
  void handlesValuesDifferentTypes() {
    Entity entity = entity(Map.of("key1", string("foo")));
    assertFalse(
        matcher.matches(entity, condition("key1", PREDICATE_OPERATOR_EQUALS, integerValue(5))));
    assertTrue(
        matcher.matches(entity, condition("key1", PREDICATE_OPERATOR_NOT_EQUALS, integerValue(5))));

    // Throws for comparisons
    assertThrows(
        IllegalArgumentException.class,
        () ->
            matcher.matches(
                entity, condition("key1", PREDICATE_OPERATOR_LESS_THAN, integerValue(5))));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            matcher.matches(
                entity, condition("key1", PREDICATE_OPERATOR_GREATER_THAN, integerValue(5))));
  }

  @Test
  void throwsIfUnknownOperator() {
    Entity entity = entity(Map.of("key1", string("foo")));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            matcher.matches(
                entity, condition("key1", PREDICATE_OPERATOR_UNSPECIFIED, string("foo"))));
  }

  private void verifyOperatorBehavior(
      AttributeValue entityValue,
      AttributeValue rhsGoodValue,
      AttributeValue rhsBadValue,
      PredicateOperator operator) {
    Entity entity = entity(Map.of("key1", entityValue));
    assertTrue(matcher.matches(entity, condition("key1", operator, rhsGoodValue)));
    assertFalse(matcher.matches(entity, condition("key1", operator, rhsBadValue)));
  }

  private AttributeValue string(String value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setString(value)).build();
  }

  private AttributeValue integerValue(int value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setInt(value)).build();
  }

  private AttributeValue booleanValue(boolean value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setBoolean(value)).build();
  }

  private AttributeValue longValue(long value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setLong(value)).build();
  }

  private AttributeValue floatValue(float value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setFloat(value)).build();
  }

  private AttributeValue doubleValue(double value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setDouble(value)).build();
  }

  private AttributeValue byteValue(String value) {
    return AttributeValue.newBuilder()
        .setValue(Value.newBuilder().setBytes(ByteString.copyFromUtf8(value)))
        .build();
  }

  private AttributeValue timestamp(long value) {
    return AttributeValue.newBuilder().setValue(Value.newBuilder().setTimestamp(value)).build();
  }

  private AttributeValue stringList(String... values) {
    return Arrays.stream(values)
        .map(this::string)
        .collect(
            Collectors.collectingAndThen(
                Collectors.toList(),
                list ->
                    AttributeValue.newBuilder()
                        .setValueList(AttributeValueList.newBuilder().addAllValues(list))
                        .build()));
  }

  private AttributeValue stringMap(Map<String, String> map) {
    return map.entrySet().stream()
        .collect(
            Collectors.collectingAndThen(
                Collectors.toUnmodifiableMap(Entry::getKey, entry -> string(entry.getValue())),
                attributeValueMap ->
                    AttributeValue.newBuilder()
                        .setValueMap(AttributeValueMap.newBuilder().putAllValues(attributeValueMap))
                        .build()));
  }

  private Entity entity(Map<String, AttributeValue> attributeValueMap) {
    return Entity.newBuilder().putAllAttributes(attributeValueMap).build();
  }

  private UpsertCondition condition(String key, PredicateOperator operator, AttributeValue value) {
    return UpsertCondition.newBuilder()
        .setPropertyPredicate(
            Predicate.newBuilder()
                .setAttributeKey(key)
                .setOperator(operator)
                .setValue(value)
                .build())
        .build();
  }
}
