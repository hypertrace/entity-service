package org.hypertrace.entity.rateLimiter;

import static org.hypertrace.entity.attribute.translator.EntityAttributeMapping.ENTITY_ATTRIBUTE_DOC_PREFIX;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.RAW_ENTITIES_COLLECTION;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
import java.time.Clock;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.attribute.translator.AttributeMetadataIdentifier;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.data.service.v1.AttributeFilter;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Operator;
import org.hypertrace.entity.rateLimiter.EntityRateLimiterConfig.RateLimitConfig;
import org.hypertrace.entity.service.change.event.impl.ChangeResult;
import org.hypertrace.entity.service.change.event.impl.EntityChangeEvaluator;
import org.hypertrace.entity.service.util.DocStoreConverter;

@Slf4j
public class EntityRateLimiter {

  private static final String DOT = ".";
  private static final String CREATED_TIME_FIELD_NAME = "createdTime";
  private final org.hypertrace.core.documentstore.Collection entitiesCollection;
  private final LoadingCache<RateLimitKey, Long> windowEntitiesCount;
  private final LoadingCache<RateLimitKey, Long> globalEntitiesCount;
  private final EntityAttributeMapping entityAttributeMapping;
  private final EntityRateLimiterConfig entityRateLimiterConfig;
  private final Clock clock;

  public EntityRateLimiter(
      Config config,
      Datastore datastore,
      EntityAttributeMapping entityAttributeMapping,
      Clock clock) {
    this.entityRateLimiterConfig = new EntityRateLimiterConfig(config);
    this.entitiesCollection = datastore.getCollection(RAW_ENTITIES_COLLECTION);
    this.entityAttributeMapping = entityAttributeMapping;
    EntityRateLimiterCacheConfig windowRateLimiterCacheConfig =
        entityRateLimiterConfig.getWindowRateLimiterCacheConfig();
    this.windowEntitiesCount =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(windowRateLimiterCacheConfig.getRefreshDuration())
            .expireAfterWrite(windowRateLimiterCacheConfig.getExpiryDuration())
            .maximumSize(windowRateLimiterCacheConfig.getMaxSize())
            .recordStats()
            .build(CacheLoader.from(this::loadWindowEntitiesCount));
    EntityRateLimiterCacheConfig globalRateLimiterCacheConfig =
        entityRateLimiterConfig.getGlobalRateLimiterCacheConfig();
    this.globalEntitiesCount =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(globalRateLimiterCacheConfig.getRefreshDuration())
            .expireAfterWrite(globalRateLimiterCacheConfig.getExpiryDuration())
            .maximumSize(globalRateLimiterCacheConfig.getMaxSize())
            .recordStats()
            .build(CacheLoader.from(this::loadGlobalEntitiesCount));
    this.clock = clock;

    PlatformMetricsRegistry.registerCache(
        this.getClass().getName() + DOT + "entitiesCount",
        globalEntitiesCount,
        Collections.emptyMap());
  }

  @VisibleForTesting
  EntityRateLimiter(
      org.hypertrace.core.documentstore.Collection entitiesCollection,
      EntityAttributeMapping entityAttributeMapping,
      EntityRateLimiterConfig entityRateLimiterConfig,
      Clock clock) {
    this.entitiesCollection = entitiesCollection;
    EntityRateLimiterCacheConfig windowRateLimiterCacheConfig =
        entityRateLimiterConfig.getWindowRateLimiterCacheConfig();
    this.windowEntitiesCount =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(windowRateLimiterCacheConfig.getRefreshDuration())
            .expireAfterWrite(windowRateLimiterCacheConfig.getExpiryDuration())
            .maximumSize(windowRateLimiterCacheConfig.getMaxSize())
            .recordStats()
            .build(CacheLoader.from(this::loadWindowEntitiesCount));
    EntityRateLimiterCacheConfig globalRateLimiterCacheConfig =
        entityRateLimiterConfig.getGlobalRateLimiterCacheConfig();
    this.globalEntitiesCount =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(globalRateLimiterCacheConfig.getRefreshDuration())
            .expireAfterWrite(globalRateLimiterCacheConfig.getExpiryDuration())
            .maximumSize(globalRateLimiterCacheConfig.getMaxSize())
            .recordStats()
            .build(CacheLoader.from(this::loadGlobalEntitiesCount));
    this.entityAttributeMapping = entityAttributeMapping;
    this.entityRateLimiterConfig = entityRateLimiterConfig;
    this.clock = clock;
  }

  public boolean isRateLimited(
      RequestContext requestContext,
      Collection<Entity> existingEntities,
      Collection<Entity> updatedEntities) {
    if (this.entityRateLimiterConfig.isDisabled()) {
      return false;
    }

    ChangeResult changeResult =
        EntityChangeEvaluator.evaluateChange(existingEntities, updatedEntities);
    Map<String, Map<Optional<String>, List<Entity>>> entityTypeToEnvToEntityList =
        changeResult.getCreatedEntity().stream()
            .collect(
                Collectors.groupingBy(
                    Entity::getEntityType,
                    Collectors.groupingBy(entity -> getEnvironment(requestContext, entity))));

    return entityTypeToEnvToEntityList.entrySet().stream()
        .anyMatch(entry -> evaluateLimits(requestContext, entry.getKey(), entry.getValue()));
  }

  private Long loadGlobalEntitiesCount(RateLimitKey rateLimitKey) {
    Optional<AttributeFilter> defaultAttributeFilter =
        this.entityRateLimiterConfig.getDefaultAttributeFilter(rateLimitKey.getEntityType());

    List<AttributeFilter> childFilters = Lists.newArrayList();
    defaultAttributeFilter.ifPresent(childFilters::add);
    addEnvironmentFilter(rateLimitKey, childFilters);
    return entitiesCollection.total(
        buildQuery(rateLimitKey.getTenantId(), rateLimitKey.getEntityType(), childFilters));
  }

  private Long loadWindowEntitiesCount(RateLimitKey rateLimitKey) {
    long timestamp = this.clock.millis() - entityRateLimiterConfig.getWindowDuration().toMillis();
    List<AttributeFilter> childFilters =
        Lists.newArrayList(
            AttributeFilter.newBuilder()
                .setName(CREATED_TIME_FIELD_NAME)
                .setOperator(Operator.GT)
                .setAttributeValue(
                    AttributeValue.newBuilder()
                        .setValue(
                            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                                .setLong(timestamp)
                                .build())
                        .build())
                .build());

    Optional<AttributeFilter> defaultAttributeFilter =
        this.entityRateLimiterConfig.getDefaultAttributeFilter(rateLimitKey.getEntityType());
    defaultAttributeFilter.ifPresent(childFilters::add);
    addEnvironmentFilter(rateLimitKey, childFilters);
    return entitiesCollection.total(
        buildQuery(rateLimitKey.getTenantId(), rateLimitKey.getEntityType(), childFilters));
  }

  private boolean evaluateLimits(
      RequestContext requestContext,
      String entityType,
      Map<Optional<String>, List<Entity>> envToEntityList) {
    return envToEntityList.entrySet().stream()
        .anyMatch(
            entry -> isRateLimited(requestContext, entityType, entry.getKey(), entry.getValue()));
  }

  private boolean isRateLimited(
      RequestContext requestContext,
      String entityType,
      Optional<String> environment,
      List<Entity> entities) {
    try {
      String tenantId = requestContext.getTenantId().orElseThrow();
      Optional<RateLimitConfig> rateLimitConfig =
          entityRateLimiterConfig.getRateLimitConfig(tenantId, environment, entityType);
      if (rateLimitConfig.isEmpty()) {
        return false;
      }

      Long globalCount =
          this.globalEntitiesCount.get(new RateLimitKey(tenantId, environment, entityType));
      Long windowEntitiesCount =
          this.windowEntitiesCount.get(new RateLimitKey(tenantId, environment, entityType));
      boolean isGlobalEntitiesLimitBreached =
          rateLimitConfig.get().getGlobalEntitiesLimit() < (globalCount + entities.size());
      boolean isTimeRangeLimitBreached =
          rateLimitConfig.get().getTimeRangeEntitiesLimit()
              < (windowEntitiesCount + entities.size());

      return isGlobalEntitiesLimitBreached || isTimeRangeLimitBreached;
    } catch (ExecutionException e) {
      log.error("Error while evaluating rate limits {}", requestContext, e);
    }
    return false;
  }

  private org.hypertrace.core.documentstore.Query buildQuery(
      String tenantId, String entityType, List<AttributeFilter> filters) {
    return DocStoreConverter.transform(
        tenantId,
        org.hypertrace.entity.data.service.v1.Query.newBuilder()
            .setFilter(
                AttributeFilter.newBuilder()
                    .setOperator(Operator.AND)
                    .addAllChildFilter(filters)
                    .build())
            .setEntityType(entityType)
            .build(),
        Collections.emptyList());
  }

  private void addEnvironmentFilter(RateLimitKey rateLimitKey, List<AttributeFilter> childFilters) {
    Optional<String> environmentAttributeId =
        this.entityRateLimiterConfig.getEnvironmentAttributeId(rateLimitKey.getEntityType());
    if (environmentAttributeId.isPresent()) {
      Optional<AttributeMetadataIdentifier> attributeMetadata =
          this.entityAttributeMapping.getAttributeMetadataByAttributeId(
              RequestContext.forTenantId(rateLimitKey.getTenantId()), environmentAttributeId.get());
      if (attributeMetadata.isPresent() && rateLimitKey.getEnvironment().isPresent()) {
        childFilters.add(
            AttributeFilter.newBuilder()
                .setName(attributeMetadata.get().getDocStorePath())
                .setOperator(Operator.EQ)
                .setAttributeValue(
                    AttributeValue.newBuilder()
                        .setValue(
                            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                                .setString(rateLimitKey.getEnvironment().get())
                                .build())
                        .build())
                .build());
      }
    }
  }

  private Optional<String> getEnvironment(RequestContext requestContext, Entity entity) {
    Optional<String> environmentAttributeId =
        this.entityRateLimiterConfig.getEnvironmentAttributeId(entity.getEntityType());
    if (environmentAttributeId.isEmpty()) {
      return Optional.empty();
    }
    Optional<AttributeMetadataIdentifier> attributeMetadata =
        this.entityAttributeMapping.getAttributeMetadataByAttributeId(
            requestContext, environmentAttributeId.get());

    if (attributeMetadata.isEmpty()) {
      return Optional.empty();
    }
    String docStorePath = attributeMetadata.get().getDocStorePath();
    Optional<AttributeValue> attributeValue =
        Optional.ofNullable(entity.getAttributesMap().get(getAttributeName(docStorePath)));
    return attributeValue.map(value -> value.getValue().getString());
  }

  private String getAttributeName(String attributeDocPath) {
    return attributeDocPath.replace(ENTITY_ATTRIBUTE_DOC_PREFIX, "");
  }
}
