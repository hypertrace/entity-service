package org.hypertrace.entity.data.service;

import static java.util.Collections.emptyList;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.ENTITY_TYPES_COLLECTION;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.entity.service.util.TenantUtils;
import org.hypertrace.entity.type.service.v1.AttributeKind;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.type.service.v1.EntityType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class IdentifyingAttributeCacheTest {

  @Mock Collection mockCollection;

  @Mock Datastore mockDataStore;

  private IdentifyingAttributeCache cache;

  @BeforeEach
  void beforeEach() {
    //    when (this.mockCollection.search(any())).then()
    when(this.mockDataStore.getCollection(ENTITY_TYPES_COLLECTION)).thenReturn(this.mockCollection);
    this.cache = new IdentifyingAttributeCache(this.mockDataStore);
  }

  @Test
  void cachesDifferentEntityTypesSameTenant() {
    when(this.mockCollection.search(
            argThat(
                (Query query) ->
                    query.getFilter().getValue().equals(TenantUtils.getTenantHierarchy("tenant")))))
        .thenReturn(
            this.buildEntityTypeResponse(
                Map.of(
                    "first-type",
                    List.of("first-attr", "second-attr"),
                    "second-type",
                    List.of("third-attr", "fourth-attr"))));
    assertEquals(
        List.of(this.buildIdAttrType("first-attr"), this.buildIdAttrType("second-attr")),
        this.cache.getIdentifyingAttributes("tenant", "first-type"));

    assertEquals(
        List.of(this.buildIdAttrType("third-attr"), this.buildIdAttrType("fourth-attr")),
        this.cache.getIdentifyingAttributes("tenant", "second-type"));
    verify(this.mockCollection, times(1)).search(any());
  }

  @Test
  void multipleConcurrentTenants() {
    // Flip around mocks because of mockito ordering issues with custom matchers
    doReturn(
            this.buildEntityTypeResponse(
                Map.of("first-type", List.of("first-attr", "second-attr"))))
        .when(this.mockCollection)
        .search(
            argThat(
                (Query query) ->
                    query
                        .getFilter()
                        .getValue()
                        .equals(TenantUtils.getTenantHierarchy("tenant-1"))));
    doReturn(
            this.buildEntityTypeResponse(
                Map.of("first-type", List.of("third-attr", "fourth-attr"))))
        .when(this.mockCollection)
        .search(
            argThat(
                (Query query) ->
                    query
                        .getFilter()
                        .getValue()
                        .equals(TenantUtils.getTenantHierarchy("tenant-2"))));

    assertEquals(
        List.of(this.buildIdAttrType("first-attr"), this.buildIdAttrType("second-attr")),
        this.cache.getIdentifyingAttributes("tenant-1", "first-type"));

    assertEquals(
        List.of(this.buildIdAttrType("third-attr"), this.buildIdAttrType("fourth-attr")),
        this.cache.getIdentifyingAttributes("tenant-2", "first-type"));
    verify(this.mockCollection, times(2)).search(any());
  }

  @Test
  void errorResponseShouldNotBeCached() {
    doThrow(UnsupportedOperationException.class).when(this.mockCollection).search(any());

    assertThrows(
        UncheckedExecutionException.class,
        () -> this.cache.getIdentifyingAttributes("tenant", "first-type"));

    doReturn(
            this.buildEntityTypeResponse(
                Map.of("first-type", List.of("first-attr", "second-attr"))))
        .when(this.mockCollection)
        .search(any());

    assertEquals(
        List.of(this.buildIdAttrType("first-attr"), this.buildIdAttrType("second-attr")),
        this.cache.getIdentifyingAttributes("tenant", "first-type"));
  }

  @Test
  void returnsEmptyListForUnknownEntityType() {
    doReturn(
            this.buildEntityTypeResponse(
                Map.of("first-type", List.of("first-attr", "second-attr"))))
        .when(this.mockCollection)
        .search(any());

    assertEquals(emptyList(), this.cache.getIdentifyingAttributes("tenant", "second-type"));
  }

  private Iterator<Document> buildEntityTypeResponse(
      Map<String, List<String>> typeToAttributeTypeNames) {
    return typeToAttributeTypeNames.entrySet().stream()
        .map(entry -> this.buildEntityTypeDocForAttribute(entry.getKey(), entry.getValue()))
        .iterator();
  }

  private Document buildEntityTypeDocForAttribute(String name, List<String> attributeTypeNames) {
    try {
      return new JSONDocument(
          JsonFormat.printer()
              .print(
                  EntityType.newBuilder()
                      .setName(name)
                      .addAllAttributeType(
                          attributeTypeNames.stream()
                              .map(this::buildIdAttrType)
                              .collect(Collectors.toList()))));
    } catch (IOException exception) {
      throw new RuntimeException(exception);
    }
  }

  private AttributeType buildIdAttrType(String name) {
    return AttributeType.newBuilder()
        .setName(name)
        .setValueKind(AttributeKind.TYPE_STRING)
        .setIdentifyingAttribute(true)
        .build();
  }
}
