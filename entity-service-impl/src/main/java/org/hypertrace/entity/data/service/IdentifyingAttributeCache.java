package org.hypertrace.entity.data.service;

import static org.hypertrace.entity.service.constants.EntityCollectionConstants.ENTITY_TYPES_COLLECTION;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Streams;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.Collection;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.entity.service.constants.EntityServiceConstants;
import org.hypertrace.entity.service.util.TenantUtils;
import org.hypertrace.entity.type.service.v1.AttributeType;
import org.hypertrace.entity.type.service.v1.EntityType;

class IdentifyingAttributeCache {
  private static final DocumentParser PARSER = new DocumentParser();
  private final LoadingCache<String, Map<String, List<AttributeType>>> cache;
  private final Collection entityTypesCollection;

  IdentifyingAttributeCache(Datastore datastore) {
    this.entityTypesCollection = datastore.getCollection(ENTITY_TYPES_COLLECTION);
    this.cache =
        CacheBuilder.newBuilder()
            .maximumWeight(1000)
            .expireAfterWrite(15, TimeUnit.MINUTES)
            .weigher((String key, Map<String, List<AttributeType>> value) -> value.size())
            .build(CacheLoader.from(this::loadEntityTypes));
  }

  List<AttributeType> getIdentifyingAttributes(String tenantId, String entityTypeName) {
    return this.cache.getUnchecked(tenantId).getOrDefault(entityTypeName, Collections.emptyList());
  }

  private Map<String, List<AttributeType>> loadEntityTypes(String tenantId) {
    Query query = new Query();
    query.setFilter(
        new Filter(
            Op.IN, EntityServiceConstants.TENANT_ID, TenantUtils.getTenantHierarchy(tenantId)));

    return Streams.stream(entityTypesCollection.search(query))
        .flatMap(this::buildEntityType)
        .collect(
            Collectors.toUnmodifiableMap(
                EntityType::getName, this::getIdentifyingAttributesFromType));
  }

  private Stream<EntityType> buildEntityType(Document doc) {
    return PARSER.<EntityType>parseOrLog(doc, EntityType.newBuilder()).stream();
  }

  private List<AttributeType> getIdentifyingAttributesFromType(EntityType entityType) {
    return entityType.getAttributeTypeList().stream()
        .filter(AttributeType::getIdentifyingAttribute)
        .collect(Collectors.toList());
  }
}
