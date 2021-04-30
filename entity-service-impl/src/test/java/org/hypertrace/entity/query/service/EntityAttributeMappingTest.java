package org.hypertrace.entity.query.service;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import io.reactivex.rxjava3.core.Single;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EntityAttributeMappingTest {

  @Mock CachingAttributeClient mockAttributeClient;
  @Mock RequestContext mockRequestContext;

  @Test
  void returnsMappingFromConfig() {
    EntityAttributeMapping attributeMapping =
        new EntityAttributeMapping(
            this.mockAttributeClient, Map.of("some-id", "attributes.some-key"));

    assertEquals(
        Optional.of("attributes.some-key"),
        attributeMapping.getDocStorePathByAttributeId(mockRequestContext, "some-id"));

    // Should have priority over attribute client
    verifyNoInteractions(mockAttributeClient);
  }

  @Test
  void returnsMappingFromAttributeService() {
    when(mockRequestContext.call(any())).thenCallRealMethod();
    EntityAttributeMapping attributeMapping =
        new EntityAttributeMapping(this.mockAttributeClient, Collections.emptyMap());
    AttributeMetadata sourcelessMetadata =
        AttributeMetadata.newBuilder().setKey("some-key").build();
    when(this.mockAttributeClient.get("some-id")).thenReturn(Single.just(sourcelessMetadata));

    // Empty result, since the mock metadata doesn't have an entity source
    assertEquals(
        Optional.empty(),
        attributeMapping.getDocStorePathByAttributeId(mockRequestContext, "some-id"));

    AttributeMetadata goodMetadata =
        sourcelessMetadata.toBuilder().addSources(AttributeSource.EDS).build();
    when(this.mockAttributeClient.get("some-id")).thenReturn(Single.just(goodMetadata));

    assertEquals(
        Optional.of("attributes.some-key"),
        attributeMapping.getDocStorePathByAttributeId(mockRequestContext, "some-id"));
  }

  @Test
  void returnsEmptyIfNoMapping() {
    when(mockRequestContext.call(any())).thenCallRealMethod();
    EntityAttributeMapping attributeMapping =
        new EntityAttributeMapping(this.mockAttributeClient, Collections.emptyMap());
    when(this.mockAttributeClient.get("some-id")).thenReturn(Single.error(new RuntimeException()));

    // Empty result, since attribute client threw error
    assertEquals(
        Optional.empty(),
        attributeMapping.getDocStorePathByAttributeId(mockRequestContext, "some-id"));
  }

  @Test
  void testIsMultiValueAttribute() {
    when(mockRequestContext.call(any())).thenCallRealMethod();
    EntityAttributeMapping attributeMapping =
        new EntityAttributeMapping(this.mockAttributeClient, Collections.emptyMap());
    AttributeMetadata singleValueAttributeData =
        AttributeMetadata.newBuilder()
            .setKey("some-key")
            .setValueKind(AttributeKind.TYPE_STRING)
            .addSources(AttributeSource.EDS)
            .build();
    when(this.mockAttributeClient.get("some-id")).thenReturn(Single.just(singleValueAttributeData));

    // Empty result, since the mock metadata doesn't have an entity source
    assertFalse(attributeMapping.isMultiValued(mockRequestContext, "some-id"));

    AttributeMetadata multiValueAttributeData =
        AttributeMetadata.newBuilder()
            .setKey("some-key")
            .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
            .addSources(AttributeSource.EDS)
            .build();
    when(this.mockAttributeClient.get("some-id")).thenReturn(Single.just(multiValueAttributeData));

    assertTrue(attributeMapping.isMultiValued(mockRequestContext, "some-id"));

    AttributeMetadata invalidSourceAttributeData =
        AttributeMetadata.newBuilder()
            .setKey("some-key")
            .setValueKind(AttributeKind.TYPE_STRING_ARRAY)
            .addSources(AttributeSource.QS)
            .build();
    when(this.mockAttributeClient.get("some-id"))
        .thenReturn(Single.just(invalidSourceAttributeData));
    assertFalse(attributeMapping.isMultiValued(mockRequestContext, "some-id"));
  }
}
