package org.hypertrace.entity.query.service.client;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;

public class EntityLabelsCachingClient implements EntityLabelsClient {
  private static final int THREAD_COUNT = 20;
  private static final int CACHE_INITIAL_CAPACITY = 200;

  private static final ExecutorService executorService = Executors.newFixedThreadPool(THREAD_COUNT);
  private static final ListeningExecutorService listeningExecutorService = MoreExecutors.listeningDecorator(executorService);

  private EntityQueryServiceClient entityQueryServiceClient;

  private final CacheLoader<EntityCacheKey<EntityTypeAndId>, List<String>> loader = new CacheLoader<>() {
    @Override
    public List<String> load(EntityCacheKey<EntityTypeAndId> key) {
      return loadEntityLabelsForKeys(List.of(key)).get(key);
    }

    @Override
    public Map<EntityCacheKey<EntityTypeAndId>, List<String>> loadAll(Iterable<? extends EntityCacheKey<EntityTypeAndId>> keys) {
      return loadEntityLabelsForKeys(keys);
    }

    @Override
    public ListenableFuture<List<String>> reload(EntityCacheKey<EntityTypeAndId> key, List<String> oldValue) {
      return listeningExecutorService.submit(() -> load(key));
    }
  };

  private final LoadingCache<EntityCacheKey<EntityTypeAndId>, List<String>> entityIdsToLabelsCache;

  public EntityLabelsCachingClient(EntityQueryServiceClient entityQueryServiceClient,
                                   EntityLabelsCachingClientConfig entityLabelsCachingClientConfig) {
    this.entityQueryServiceClient = entityQueryServiceClient;
    this.entityIdsToLabelsCache = CacheBuilder.newBuilder()
        .initialCapacity(CACHE_INITIAL_CAPACITY)
        .maximumSize(entityLabelsCachingClientConfig.getMaximumSize())
        .expireAfterWrite(entityLabelsCachingClientConfig.getExpiryTimeInMinutes(), TimeUnit.MINUTES)
        .build(loader);
  }

  @Override
  public List<String> getEntityLabelsForEntity(String idColumnName,
                                               String labelsColumnName,
                                               String id,
                                               String type,
                                               Map<String, String> headers,
                                               String tenantId) {
    try {
      return entityIdsToLabelsCache.get(
          new EntityCacheKey<>(new EntityTypeAndId(type, id, idColumnName, labelsColumnName),
              tenantId,
              headers
          )
      );
    } catch (ExecutionException ex) {
      throw new RuntimeException("EntityLabelsCachingClient exception while getting labels for an entity", ex);
    }
  }

  @Override
  public void refreshEntityLabelsForEntity(String idColumnName,
                                           String labelsColumnName,
                                           String id,
                                           String type,
                                           Map<String, String> headers,
                                           String tenantId) {
    entityIdsToLabelsCache.refresh(
        new EntityCacheKey<>(new EntityTypeAndId(type, id, idColumnName, labelsColumnName),
            tenantId,
            headers
        )
    );
  }

  @Override
  public Map<String, List<String>> getEntityLabelsForEntities(String idColumnName,
                                                              String labelsColumnName,
                                                              List<String> ids,
                                                              String type,
                                                              Map<String, String> headers,
                                                              String tenantId) {
    List<EntityCacheKey<EntityTypeAndId>> entityCacheKeys = ids.stream()
        .map(entityId -> new EntityTypeAndId(type, entityId, idColumnName, labelsColumnName))
        .map(entityTypeAndId -> new EntityCacheKey<>(entityTypeAndId, tenantId, headers))
        .collect(Collectors.toList());
    try {
      Map<EntityCacheKey<EntityTypeAndId>, List<String>> entityCacheKeyToLabelsMap =  entityIdsToLabelsCache.getAll(entityCacheKeys);
      return entityCacheKeyToLabelsMap.entrySet().stream()
          .collect(
              Collectors.toMap(entry -> entry.getKey().getDataKey().getId(), Entry::getValue)
          );
    } catch (ExecutionException ex) {
      throw new RuntimeException("EntityLabelsCachingClient exception while getting labels for entities", ex);
    }
  }

  private Map<EntityCacheKey<EntityTypeAndId>, List<String>> loadEntityLabelsForKeys(Iterable<? extends EntityCacheKey<EntityTypeAndId>> keys) {
    Iterator<? extends EntityCacheKey<EntityTypeAndId>> iter = keys.iterator();
    if (!iter.hasNext()) {
      return Map.of();
    }

    List<EntityCacheKey<EntityTypeAndId>> entityIdKeys = new ArrayList<>();
    while (iter.hasNext()) {
      entityIdKeys.add(iter.next());
    }

    // The assumption we make is that the all the entity id keys are of the same entity type and
    // belong to the same tenant. So we can extract those values from the first cache key.
    EntityCacheKey<EntityTypeAndId> firstEntityCacheKey = entityIdKeys.get(0);
    String entityType = firstEntityCacheKey.getDataKey().getType();
    String entityIdColumnName = firstEntityCacheKey.getDataKey().getIdColumnName();
    String entityLabelsColumnName = firstEntityCacheKey.getDataKey().getLabelsColumnName();
    Map<String, String> headers = firstEntityCacheKey.getHeaders();

    Map<String, EntityCacheKey<EntityTypeAndId>> entityIdToCacheKey = new HashMap<>();
    List<String> entityIds = entityIdKeys.stream()
        .map(key -> {
          entityIdToCacheKey.put(key.getDataKey().getId(), key);
          return key.getDataKey().getId();
        })
        .collect(Collectors.toList());

    EntityQueryRequest entityQueryRequest = EntityQueryRequest.newBuilder()
        .setEntityType(entityType)
        .addSelection(createColumnExpression(entityIdColumnName))
        .addSelection(createColumnExpression(entityLabelsColumnName))
        .setFilter(
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(
                    Filter.newBuilder()
                        .setOperator(Operator.IN)
                        .setLhs(createColumnExpression(entityIdColumnName))
                        .setRhs(createStringArrayLiteralExpression(entityIds)
                        )
                )
        )
        .setLimit(entityIds.size())
        .build();

    Iterator<ResultSetChunk> resultSetChunkIterator =
        entityQueryServiceClient.execute(entityQueryRequest, headers);

    Map<EntityCacheKey<EntityTypeAndId>, List<String>> entityLabelsByEntityIdMap = new HashMap<>();
    while (resultSetChunkIterator.hasNext()) {
      ResultSetChunk chunk = resultSetChunkIterator.next();

      if (chunk.getRowCount() < 1) {
        break;
      }

      for (Row row : chunk.getRowList()) {
        String entityId = row.getColumn(0).getString();
        List<String> labels = row.getColumn(1).getStringArrayList();

        entityLabelsByEntityIdMap.put(entityIdToCacheKey.get(entityId), labels);
      }
    }

    // For entity ids that were not found in entity-service, set the labels to empty list
    // TODO: This is to take care of the issue with not filtering on customerId as well for old
    //    entities that do not have the tenantId field name in the entity document.
    for (EntityCacheKey<EntityTypeAndId> entityIdCacheKey : entityIdKeys) {
      if (!entityLabelsByEntityIdMap.containsKey(entityIdCacheKey)) {
        entityLabelsByEntityIdMap.put(entityIdCacheKey, List.of());
      }
    }

    return entityLabelsByEntityIdMap;
  }

  private static Expression.Builder createColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName));
  }

  private static Expression.Builder createStringArrayLiteralExpression(List<String> strList) {
    return Expression.newBuilder().setLiteral(
        LiteralConstant.newBuilder().setValue(
            Value.newBuilder()
                .setValueType(ValueType.STRING_ARRAY)
                .addAllStringArray(strList)
        )
    );
  }

  @Override
  public List<String> getEntityIdsByLabels(String idColumnName,
                                           String labelsColumnName,
                                           List<String> labels,
                                           String type,
                                           Map<String, String> headers,
                                           String tenantId) {
    throw new UnsupportedOperationException("getEntityIdsByLabels not implemented yet");
  }
}
