package org.hypertrace.entity.fetcher;

import java.io.IOException;
import java.util.List;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.JSONDocument;
import org.hypertrace.entity.TestUtils;
import org.hypertrace.entity.data.service.DocumentParser;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class EntityFetcherTest {
  private static final String TENANT_ID = "tenant1";

  private Collection entityCollection;
  private EntityFetcher entityFetcher;

  @BeforeEach
  void setup() {
    this.entityCollection = Mockito.mock(Collection.class);
    this.entityFetcher = new EntityFetcher(entityCollection, new DocumentParser());
  }

  @Test
  void testGetByEntityIds() throws IOException {
    List<Document> docs =
        List.of(new JSONDocument("{\n" + "    \"entityId\": \"entityId1\"" + "}"));
    Mockito.when(
            this.entityCollection.search(
                buildExistingEntitiesByEntityIdQuery(List.of("entityId1"))))
        .thenReturn(TestUtils.convertToCloseableIterator(docs.iterator()));
    List<Entity> entities =
        this.entityFetcher.getEntitiesByEntityIds(TENANT_ID, List.of("entityId1"));
    Assertions.assertEquals(1, entities.size());
    Assertions.assertEquals(entities.get(0).getEntityId(), "entityId1");
  }

  @Test
  void testGetByDocIds() throws IOException {
    List<Document> docs =
        List.of(new JSONDocument("{\n" + "    \"entityId\": \"entityId1\"" + "}"));
    Mockito.when(this.entityCollection.search(buildExistingEntitiesByDocIdQuery(List.of("docId"))))
        .thenReturn(TestUtils.convertToCloseableIterator(docs.iterator()));
    List<Entity> entities = this.entityFetcher.getEntitiesByDocIds(TENANT_ID, List.of("docId"));
    Assertions.assertEquals(1, entities.size());
    Assertions.assertEquals(entities.get(0).getEntityId(), "entityId1");
  }

  private org.hypertrace.core.documentstore.Query buildExistingEntitiesByDocIdQuery(
      java.util.Collection<String> docIds) {
    org.hypertrace.core.documentstore.Query query = new org.hypertrace.core.documentstore.Query();
    query.setFilter(
        new Filter(
            Op.AND,
            null,
            null,
            new Filter(Op.EQ, EntityServiceConstants.TENANT_ID, TENANT_ID),
            new Filter(Op.IN, EntityServiceConstants.ID, docIds)));
    return query;
  }

  private org.hypertrace.core.documentstore.Query buildExistingEntitiesByEntityIdQuery(
      java.util.Collection<String> docIds) {
    org.hypertrace.core.documentstore.Query query = new org.hypertrace.core.documentstore.Query();
    query.setFilter(
        new Filter(
            Op.AND,
            null,
            null,
            new Filter(Op.EQ, EntityServiceConstants.TENANT_ID, TENANT_ID),
            new Filter(Op.IN, EntityServiceConstants.ENTITY_ID, docIds)));
    return query;
  }
}
