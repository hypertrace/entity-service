package org.hypertrace.entity.fetcher;

import com.google.common.collect.Streams;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.entity.data.service.DocumentParser;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.constants.EntityServiceConstants;

public class EntityFetcher {

  private final DocumentParser parser;
  private final Collection entitiesCollection;

  public EntityFetcher(Collection entitiesCollection, DocumentParser documentParser) {
    this.entitiesCollection = entitiesCollection;
    this.parser = documentParser;
  }

  public List<Entity> getEntitiesByDocIds(java.util.Collection<String> docIds) {
    return query(buildExistingEntitiesByDocIdQuery(docIds)).collect(Collectors.toList());
  }

  public List<Entity> getEntitiesByEntityIds(java.util.Collection<String> entityIds) {
    return query(buildExistingEntitiesByEntityIdQuery(entityIds)).collect(Collectors.toList());
  }

  public Stream<Entity> query(org.hypertrace.core.documentstore.Query query) {
    return Streams.stream(this.entitiesCollection.search(query))
        .map(this::entityFromDocument)
        .flatMap(Optional::stream)
        .map(Entity::toBuilder)
        .map(Entity.Builder::build);
  }

  public Stream<Entity> query(org.hypertrace.core.documentstore.query.Query query) {
    return Streams.stream(this.entitiesCollection.find(query))
        .map(this::entityFromDocument)
        .flatMap(Optional::stream)
        .map(Entity::toBuilder)
        .map(Entity.Builder::build);
  }

  private org.hypertrace.core.documentstore.Query buildExistingEntitiesByDocIdQuery(
      java.util.Collection<String> docIds) {
    org.hypertrace.core.documentstore.Query query = new org.hypertrace.core.documentstore.Query();
    query.setFilter(
        new org.hypertrace.core.documentstore.Filter(
            org.hypertrace.core.documentstore.Filter.Op.IN, EntityServiceConstants.ID, docIds));
    return query;
  }

  private org.hypertrace.core.documentstore.Query buildExistingEntitiesByEntityIdQuery(
      java.util.Collection<String> docIds) {
    org.hypertrace.core.documentstore.Query query = new org.hypertrace.core.documentstore.Query();
    query.setFilter(
        new org.hypertrace.core.documentstore.Filter(
            org.hypertrace.core.documentstore.Filter.Op.IN,
            EntityServiceConstants.ENTITY_ID,
            docIds));
    return query;
  }

  private Optional<Entity> entityFromDocument(Document document) {
    return parser.parseOrLog(document, Entity.newBuilder());
  }
}
