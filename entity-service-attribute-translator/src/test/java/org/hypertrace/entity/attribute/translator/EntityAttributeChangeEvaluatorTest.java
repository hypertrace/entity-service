package org.hypertrace.entity.attribute.translator;

import static org.hypertrace.entity.attribute.translator.EntityAttributeMapping.ENTITY_ATTRIBUTE_DOC_PREFIX;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.SetAttribute;
import org.hypertrace.entity.query.service.v1.UpdateOperation;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class EntityAttributeChangeEvaluatorTest {

  private static final RequestContext requestContext = RequestContext.forTenantId("tenantId");
  private static final String TEST_ENTITY_TYPE = "test-entity-type";
  private EntityAttributeChangeEvaluator entityAttributeChangeEvaluator;
  @Mock EntityAttributeMapping entityAttributeMapping;

  @BeforeEach
  void setup() {
    Config config =
        ConfigFactory.parseMap(
            Map.of(
                "entity.service.change.skip.attributes",
                List.of(
                    TEST_ENTITY_TYPE + ".skip_attribute", TEST_ENTITY_TYPE + ".skip_attribute_1")));
    this.entityAttributeChangeEvaluator =
        new EntityAttributeChangeEvaluator(config, entityAttributeMapping);
  }

  @Test
  void testShouldSendNotification_entities() {
    // attributes differ
    when(this.entityAttributeMapping.getAttributeMetadataByAttributeId(
            any(), eq(TEST_ENTITY_TYPE + ".skip_attribute")))
        .thenReturn(
            Optional.of(
                new AttributeMetadataIdentifier(
                    TEST_ENTITY_TYPE, ENTITY_ATTRIBUTE_DOC_PREFIX + "skip_attribute")));
    Entity prevEntity =
        Entity.newBuilder()
            .setEntityType(TEST_ENTITY_TYPE)
            .putAllAttributes(
                Map.of(
                    "attribute_key",
                    AttributeValue.newBuilder()
                        .setValue(Value.newBuilder().setString("value").build())
                        .build(),
                    "skip_attribute",
                    AttributeValue.newBuilder()
                        .setValue(Value.newBuilder().setString("skip_value").build())
                        .build()))
            .build();
    Entity updatedEntity =
        prevEntity.toBuilder()
            .putAttributes(
                "attribute_key_1",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("value").build())
                    .build())
            .build();
    Assertions.assertTrue(
        entityAttributeChangeEvaluator.shouldSendNotification(
            requestContext, prevEntity, updatedEntity));

    // same entities
    prevEntity = Entity.newBuilder().setEntityType(TEST_ENTITY_TYPE).build();
    updatedEntity = Entity.newBuilder().setEntityType(TEST_ENTITY_TYPE).build();
    Assertions.assertFalse(
        entityAttributeChangeEvaluator.shouldSendNotification(
            requestContext, prevEntity, updatedEntity));

    // no attributes change
    prevEntity =
        Entity.newBuilder().setEntityType(TEST_ENTITY_TYPE).setEntityName("oldName").build();
    updatedEntity =
        Entity.newBuilder().setEntityType(TEST_ENTITY_TYPE).setEntityName("newName").build();
    Assertions.assertFalse(
        entityAttributeChangeEvaluator.shouldSendNotification(
            requestContext, prevEntity, updatedEntity));
  }

  @Test
  void testShouldSendNotification_updateOp() {
    Assertions.assertTrue(
        entityAttributeChangeEvaluator.shouldSendNotification(
            requestContext,
            TEST_ENTITY_TYPE,
            UpdateOperation.newBuilder()
                .setSetAttribute(
                    SetAttribute.newBuilder()
                        .setAttribute(
                            ColumnIdentifier.newBuilder()
                                .setColumnName(TEST_ENTITY_TYPE + ".attribute")
                                .build())
                        .build())
                .build()));

    Assertions.assertFalse(
        entityAttributeChangeEvaluator.shouldSendNotification(
            requestContext,
            TEST_ENTITY_TYPE,
            UpdateOperation.newBuilder()
                .setSetAttribute(
                    SetAttribute.newBuilder()
                        .setAttribute(
                            ColumnIdentifier.newBuilder()
                                .setColumnName(TEST_ENTITY_TYPE + ".skip_attribute")
                                .build())
                        .build())
                .build()));
  }

  @Test
  void testShouldSendNotification_updateOps() {
    Assertions.assertTrue(
        entityAttributeChangeEvaluator.shouldSendNotification(
            requestContext,
            TEST_ENTITY_TYPE,
            List.of(
                UpdateOperation.newBuilder()
                    .setSetAttribute(
                        SetAttribute.newBuilder()
                            .setAttribute(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(TEST_ENTITY_TYPE + ".attribute")
                                    .build())
                            .build())
                    .build(),
                UpdateOperation.newBuilder()
                    .setSetAttribute(
                        SetAttribute.newBuilder()
                            .setAttribute(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(TEST_ENTITY_TYPE + ".skip_attribute")
                                    .build())
                            .build())
                    .build())));

    Assertions.assertFalse(
        entityAttributeChangeEvaluator.shouldSendNotification(
            requestContext,
            TEST_ENTITY_TYPE,
            List.of(
                UpdateOperation.newBuilder()
                    .setSetAttribute(
                        SetAttribute.newBuilder()
                            .setAttribute(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(TEST_ENTITY_TYPE + ".skip_attribute")
                                    .build())
                            .build())
                    .build(),
                UpdateOperation.newBuilder()
                    .setSetAttribute(
                        SetAttribute.newBuilder()
                            .setAttribute(
                                ColumnIdentifier.newBuilder()
                                    .setColumnName(TEST_ENTITY_TYPE + ".skip_attribute_1")
                                    .build())
                            .build())
                    .build())));
  }
}
