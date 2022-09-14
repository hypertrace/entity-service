package org.hypertrace.entity.query.service.converter.filter;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import java.util.Optional;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.expression.type.FilterTypeExpression;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.converter.Converter;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class NullFilteringExpressionConverterTest {
  private static final RequestContext REQUEST_CONTEXT = RequestContext.forTenantId("tenant1");

  @Mock private EntityAttributeMapping entityAttributeMapping;
  @Mock private Converter<LiteralConstant, ConstantExpression> constantExpressionConverter;

  private NullFilteringExpressionConverter nullFilteringExpressionConverter;

  @BeforeEach
  void setup() {
    nullFilteringExpressionConverter =
        new NullFilteringExpressionConverter(entityAttributeMapping, constantExpressionConverter);
  }

  @Test
  void testEqNull() throws ConversionException {
    ColumnIdentifier columnIdentifier =
        ColumnIdentifier.newBuilder().setColumnName("column1").build();
    LiteralConstant constant =
        LiteralConstant.newBuilder().setValue(Value.newBuilder().setString("null").build()).build();

    when(entityAttributeMapping.getDocStorePathByAttributeId(REQUEST_CONTEXT, "column1"))
        .thenReturn(Optional.of("attributes.subDocPath1"));
    when(entityAttributeMapping.isMultiValued(REQUEST_CONTEXT, "column1")).thenReturn(false);

    ConstantExpression constantExpression = ConstantExpression.of("null");
    when(constantExpressionConverter.convert(constant, REQUEST_CONTEXT))
        .thenReturn(constantExpression);

    FilterTypeExpression filterTypeExpression =
        nullFilteringExpressionConverter.convert(
            columnIdentifier, Operator.EQ, constant, REQUEST_CONTEXT);
    IdentifierExpression identifierExpression = IdentifierExpression.of("attributes.subDocPath1");

    assertEquals(
        RelationalExpression.of(
            identifierExpression, RelationalOperator.NOT_EXISTS, constantExpression),
        filterTypeExpression);
  }

  @Test
  void testNeqNull() throws ConversionException {
    ColumnIdentifier columnIdentifier =
        ColumnIdentifier.newBuilder().setColumnName("column1").build();
    LiteralConstant constant =
        LiteralConstant.newBuilder().setValue(Value.newBuilder().setString("null").build()).build();

    when(entityAttributeMapping.getDocStorePathByAttributeId(REQUEST_CONTEXT, "column1"))
        .thenReturn(Optional.of("attributes.subDocPath1"));
    when(entityAttributeMapping.isMultiValued(REQUEST_CONTEXT, "column1")).thenReturn(false);

    ConstantExpression constantExpression = ConstantExpression.of("null");
    when(constantExpressionConverter.convert(constant, REQUEST_CONTEXT))
        .thenReturn(constantExpression);

    FilterTypeExpression filterTypeExpression =
        nullFilteringExpressionConverter.convert(
            columnIdentifier, Operator.NEQ, constant, REQUEST_CONTEXT);
    IdentifierExpression identifierExpression = IdentifierExpression.of("attributes.subDocPath1");

    assertEquals(
        RelationalExpression.of(
            identifierExpression, RelationalOperator.EXISTS, constantExpression),
        filterTypeExpression);
  }

  @Test
  void testArrayEqNull() throws ConversionException {
    ColumnIdentifier columnIdentifier =
        ColumnIdentifier.newBuilder().setColumnName("column1").build();
    LiteralConstant constant =
        LiteralConstant.newBuilder().setValue(Value.newBuilder().setString("null").build()).build();

    when(entityAttributeMapping.getDocStorePathByAttributeId(REQUEST_CONTEXT, "column1"))
        .thenReturn(Optional.of("attributes.subDocPath1"));
    when(entityAttributeMapping.isMultiValued(REQUEST_CONTEXT, "column1")).thenReturn(true);

    ConstantExpression constantExpression = ConstantExpression.of("null");
    when(constantExpressionConverter.convert(constant, REQUEST_CONTEXT))
        .thenReturn(constantExpression);

    FilterTypeExpression filterTypeExpression =
        nullFilteringExpressionConverter.convert(
            columnIdentifier, Operator.EQ, constant, REQUEST_CONTEXT);
    IdentifierExpression identifierExpression =
        IdentifierExpression.of("attributes.subDocPath1.valueList.values");

    assertEquals(
        RelationalExpression.of(
            identifierExpression, RelationalOperator.NOT_EXISTS, constantExpression),
        filterTypeExpression);
  }

  @Test
  void testArrayNeqNull() throws ConversionException {
    ColumnIdentifier columnIdentifier =
        ColumnIdentifier.newBuilder().setColumnName("column1").build();
    LiteralConstant constant =
        LiteralConstant.newBuilder().setValue(Value.newBuilder().setString("null").build()).build();

    when(entityAttributeMapping.getDocStorePathByAttributeId(REQUEST_CONTEXT, "column1"))
        .thenReturn(Optional.of("attributes.subDocPath1"));
    when(entityAttributeMapping.isMultiValued(REQUEST_CONTEXT, "column1")).thenReturn(true);

    ConstantExpression constantExpression = ConstantExpression.of("null");
    when(constantExpressionConverter.convert(constant, REQUEST_CONTEXT))
        .thenReturn(constantExpression);

    FilterTypeExpression filterTypeExpression =
        nullFilteringExpressionConverter.convert(
            columnIdentifier, Operator.NEQ, constant, REQUEST_CONTEXT);
    IdentifierExpression identifierExpression =
        IdentifierExpression.of("attributes.subDocPath1.valueList.values");

    assertEquals(
        RelationalExpression.of(
            identifierExpression, RelationalOperator.EXISTS, constantExpression),
        filterTypeExpression);
  }
}
