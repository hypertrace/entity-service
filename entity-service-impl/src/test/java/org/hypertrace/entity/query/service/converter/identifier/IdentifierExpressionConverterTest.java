package org.hypertrace.entity.query.service.converter.identifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;
import static org.mockito.quality.Strictness.LENIENT;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.common.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.converter.ValueHelper;
import org.hypertrace.entity.query.service.converter.accessor.ValueOneOfAccessor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class IdentifierExpressionConverterTest {
  private final RequestContext requestContext = RequestContext.forTenantId("Martian");
  private final ArrayPathSuffixAddingIdentifierConverter arrayPathSuffixAddingIdentifierConverter =
      new ArrayPathSuffixAddingIdentifierConverter(new ValueHelper(new ValueOneOfAccessor()));

  @Mock private EntityAttributeMapping attributeMapping;

  private final ColumnIdentifier columnIdentifier =
      ColumnIdentifier.newBuilder().setColumnName("planet").build();
  private Converter<ColumnIdentifier, IdentifierExpression> identifierExpressionConverter;

  @BeforeEach
  void setup() {
    identifierExpressionConverter =
        new IdentifierExpressionConverter(
            attributeMapping, arrayPathSuffixAddingIdentifierConverter);
  }

  @Test
  void testConvertWithEmptySubDocPath() {
    when(attributeMapping.getDocStorePathByAttributeId(
            requestContext, columnIdentifier.getColumnName()))
        .thenReturn(Optional.empty());
    assertThrows(
        ConversionException.class,
        () -> identifierExpressionConverter.convert(columnIdentifier, requestContext));
  }

  @Test
  void testConvert() throws ConversionException {
    when(attributeMapping.getDocStorePathByAttributeId(
            requestContext, columnIdentifier.getColumnName()))
        .thenReturn(Optional.of("attributes.entity_name"));
    IdentifierExpression expected = IdentifierExpression.of("attributes.entity_name");
    assertEquals(expected, identifierExpressionConverter.convert(columnIdentifier, requestContext));
  }

  @Test
  void testConvertArray() throws ConversionException {
    when(attributeMapping.getDocStorePathByAttributeId(
            requestContext, columnIdentifier.getColumnName()))
        .thenReturn(Optional.of("attributes.entity_name"));
    when(attributeMapping.isMultiValued(requestContext, columnIdentifier.getColumnName()))
        .thenReturn(true);
    IdentifierExpression expected =
        IdentifierExpression.of("attributes.entity_name.valueList.values");
    assertEquals(expected, identifierExpressionConverter.convert(columnIdentifier, requestContext));
  }
}
