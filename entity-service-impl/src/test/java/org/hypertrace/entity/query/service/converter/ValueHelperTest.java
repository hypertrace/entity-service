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
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.model.subdoc.SubDocumentValue;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
class ValueHelperTest {

  @Mock private RequestContext context;
  private OneOfAccessor<Value, ValueType> mockValueAccessor;
  private EntityAttributeMapping mockEntityAttributeMapping;

  private ValueHelper valueHelper;

  @SuppressWarnings("unchecked")
  @BeforeAll
  void setUp() {
    mockValueAccessor = mock(OneOfAccessor.class);
    mockEntityAttributeMapping = mock(EntityAttributeMapping.class);
    valueHelper = spy(new ValueHelper(mockValueAccessor, mockEntityAttributeMapping));
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
      final String dataType = "string";
      when(mockEntityAttributeMapping.getAttributeKind(context, dataType))
          .thenReturn(Optional.of(AttributeKind.TYPE_STRING));
      when(mockEntityAttributeMapping.isPrimitive(AttributeKind.TYPE_STRING)).thenReturn(true);
      final Optional<AttributeKind> attributeKind =
          mockEntityAttributeMapping.getAttributeKind(context, dataType);
      assertEquals(
          SubDocumentValue.of(new JSONDocument("{ \"value\": {\"" + dataType + "\": \"Mars\" }}")),
          valueHelper.convertToSubDocumentValue(
              attributeKind.get(),
              Value.newBuilder().setString("Mars").setValueType(ValueType.STRING).build()));
    }

    @Test
    void testConvertBooleanValueToSubDocumentValue() throws ConversionException, IOException {
      final String dataType = "boolean";
      when(mockEntityAttributeMapping.getAttributeKind(context, dataType))
          .thenReturn(Optional.of(AttributeKind.TYPE_BOOL));
      when(mockEntityAttributeMapping.isPrimitive(AttributeKind.TYPE_BOOL)).thenReturn(true);
      final Optional<AttributeKind> attributeKind =
          mockEntityAttributeMapping.getAttributeKind(context, dataType);
      assertEquals(
          SubDocumentValue.of(new JSONDocument("{ \"value\": {\"" + dataType + "\": true }}")),
          valueHelper.convertToSubDocumentValue(
              attributeKind.get(),
              Value.newBuilder().setBoolean(true).setValueType(ValueType.BOOL).build()));
    }

    @Test
    void testConvertBooleanDefaultValueToSubDocumentValue()
        throws ConversionException, IOException {
      final String dataType = "boolean";
      when(mockEntityAttributeMapping.getAttributeKind(context, dataType))
          .thenReturn(Optional.of(AttributeKind.TYPE_BOOL));
      when(mockEntityAttributeMapping.isPrimitive(AttributeKind.TYPE_BOOL)).thenReturn(true);
      final Optional<AttributeKind> attributeKind =
          mockEntityAttributeMapping.getAttributeKind(context, dataType);
      assertEquals(
          SubDocumentValue.of(new JSONDocument("{ \"value\": {\"boolean\": false }}")),
          valueHelper.convertToSubDocumentValue(
              attributeKind.get(),
              Value.newBuilder().setBoolean(false).setValueType(ValueType.BOOL).build()));
    }
  }
}
