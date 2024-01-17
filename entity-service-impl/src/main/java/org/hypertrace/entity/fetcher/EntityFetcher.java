package org.hypertrace.entity.fetcher;

import static java.util.stream.Collectors.toUnmodifiableList;

import com.google.common.collect.Streams;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.CloseableIterator;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.entity.data.service.DocumentParser;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityServiceConstants;

public class EntityFetcher {

  private final DocumentParser parser;
  private final org.hypertrace.core.documentstore.Collection entitiesCollection;

  public EntityFetcher(
      org.hypertrace.core.documentstore.Collection entitiesCollection,
      DocumentParser documentParser) {
    this.entitiesCollection = entitiesCollection;
    this.parser = documentParser;
  }

  public List<Entity> getEntitiesByDocIds(String tenantId, Collection<String> docIds)
      throws IOException {
    return query(buildExistingEntitiesByDocIdQuery(tenantId, docIds)).collect(toUnmodifiableList());
  }

  public List<Entity> getEntitiesByEntityIds(
      String tenantId, java.util.Collection<String> entityIds) throws IOException {
    if (entityIds.isEmpty()) {
      return Collections.emptyList();
    }
    return query(buildExistingEntitiesByEntityIdQuery(tenantId, entityIds))
        .collect(toUnmodifiableList());
  }

  public Stream<Entity> query(org.hypertrace.core.documentstore.Query query) throws IOException {
    final CloseableIterator<Document> iterator = this.entitiesCollection.search(query);
    try {
      return Streams.stream(iterator)
          .map(this::entityFromDocument)
          .flatMap(Optional::stream)
          .map(Entity::toBuilder)
          .map(Entity.Builder::build);
    } catch (final Exception e) {
      iterator.close();
      throw e;
    }
  }

  public List<Entity> query(org.hypertrace.core.documentstore.query.Query query)
      throws IOException {
    try (final CloseableIterator<Document> iterator = this.entitiesCollection.aggregate(query)) {
      return Streams.stream(iterator)
          .map(this::entityFromDocument)
          .flatMap(Optional::stream)
          .map(Entity::toBuilder)
          .map(Entity.Builder::build)
          .collect(toUnmodifiableList());
    }
  }

  private org.hypertrace.core.documentstore.Query buildExistingEntitiesByDocIdQuery(
      String tenantId, Collection<String> docIds) {
    Query query = new Query();
    query.setFilter(
        new Filter(
            Op.AND,
            null,
            null,
            new Filter(Op.EQ, EntityServiceConstants.TENANT_ID, tenantId),
            new Filter(Op.IN, EntityServiceConstants.ID, docIds)));
    return query;
  }

  private org.hypertrace.core.documentstore.Query buildExistingEntitiesByEntityIdQuery(
      String tenantId, Collection<String> docIds) {
    Query query = new Query();
    query.setFilter(
        new Filter(
            Op.AND,
            null,
            null,
            new Filter(Op.EQ, EntityServiceConstants.TENANT_ID, tenantId),
            new Filter(Op.IN, EntityServiceConstants.ENTITY_ID, docIds)));
    return query;
  }

  private Optional<Entity> entityFromDocument(Document document) {
    return parser.parseOrLog(document, Entity.newBuilder());
  }
}
