package org.hypertrace.entity.query.service.converter;

import static java.util.function.Predicate.not;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.EnumSource.Mode.INCLUDE;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.entity.query.service.converter.accessor.OneOfAccessor;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
class ValueHelperTest {
  private OneOfAccessor<Value, ValueType> mockValueAccessor;

  private ValueHelper valueHelper;

  @SuppressWarnings("unchecked")
  @BeforeAll
  void setUp() {
    mockValueAccessor = mock(OneOfAccessor.class);
    valueHelper = spy(new ValueHelper(mockValueAccessor));
  }

  @Nested
  @TestInstance(Lifecycle.PER_CLASS)
  class IsPrimitiveTest {
    @Captor private ArgumentCaptor<ValueType> typeCaptor;

    @AfterAll
    void ensureAllValueTypesCovered() {
      verify(valueHelper, atLeastOnce()).isPrimitive(typeCaptor.capture());
      final Set<ValueType> coveredTypes =
          typeCaptor.getAllValues().stream().collect(toUnmodifiableSet());
      final Set<ValueType> allTypes =
          Arrays.stream(ValueType.values())
              .filter(not(ValueType.UNRECOGNIZED::equals))
              .collect(toUnmodifiableSet());
      assertEquals(allTypes, coveredTypes);
    }

    @ParameterizedTest
    @EnumSource(
        value = ValueType.class,
        mode = INCLUDE,
        names = {"STRING", "LONG", "INT", "FLOAT", "DOUBLE", "BYTES", "BOOL", "TIMESTAMP"})
    void testIsPrimitiveForAllPrimitive(final ValueType type) {
      assertTrue(valueHelper.isPrimitive(type));
    }

    @ParameterizedTest
    @EnumSource(
        value = ValueType.class,
        mode = INCLUDE,
        names = {
          "STRING_ARRAY",
          "LONG_ARRAY",
          "INT_ARRAY",
          "FLOAT_ARRAY",
          "DOUBLE_ARRAY",
          "BYTES_ARRAY",
          "BOOLEAN_ARRAY",
          "STRING_MAP",
          "VALUE_MAP",
          "VALUE_ARRAY"
        })
    void testIsPrimitiveForAllNonPrimitive(final ValueType type) {
      assertFalse(valueHelper.isPrimitive(type));
    }
  }

  @Nested
  class ConvertToSubDocumentValueTest {
    @Test
    void testConvertStringValueToSubDocumentValue() throws ConversionException, IOException {
      assertEquals(
          SubDocumentValue.of(new JSONDocument("{ \"value\": {\"string\": \"Mars\" }}")),
          valueHelper.convertToSubDocumentValue(
              Value.newBuilder().setString("Mars").setValueType(ValueType.STRING).build()));
    }

    @Test
    void testConvertMapValueToSubDocumentValue() throws ConversionException, IOException {
      assertEquals(
          SubDocumentValue.of(new JSONDocument("{ \"key\": {\"value\": {\"string\": \"value\" }}}")),
          valueHelper.convertToSubDocumentValue(
              Value.newBuilder().putAllStringMap(Map.of("key", "value")).setValueType(ValueType.STRING_MAP).build()));
    }

    @Test
    void testConvertBooleanValueToSubDocumentValue() throws ConversionException, IOException {
      assertEquals(
          SubDocumentValue.of(new JSONDocument("{ \"value\": {\"boolean\": true }}")),
          valueHelper.convertToSubDocumentValue(
              Value.newBuilder().setBoolean(true).setValueType(ValueType.BOOL).build()));
    }

    @Test
    void testConvertBooleanDefaultValueToSubDocumentValue()
        throws ConversionException, IOException {
      assertEquals(
          SubDocumentValue.of(new JSONDocument("{ \"value\": {\"boolean\": false }}")),
          valueHelper.convertToSubDocumentValue(
              Value.newBuilder().setBoolean(false).setValueType(ValueType.BOOL).build()));
    }
  }
}
