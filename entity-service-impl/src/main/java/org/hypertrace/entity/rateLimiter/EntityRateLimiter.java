package org.hypertrace.entity.rateLimiter;

import static org.hypertrace.entity.attribute.translator.EntityAttributeMapping.ENTITY_ATTRIBUTE_DOC_PREFIX;
import static org.hypertrace.entity.service.constants.EntityCollectionConstants.RAW_ENTITIES_COLLECTION;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import com.typesafe.config.Config;
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
  private final org.hypertrace.core.documentstore.Collection entitiesCollection;
  private final LoadingCache<RateLimitKey, Long> windowEntitiesCount;
  private final LoadingCache<RateLimitKey, Long> globalEntitiesCount;
  private final EntityAttributeMapping entityAttributeMapping;
  private final EntityRateLimiterConfig entityRateLimiterConfig;

  public EntityRateLimiter(
      Config config, Datastore datastore, EntityAttributeMapping entityAttributeMapping) {
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
            .build(CacheLoader.from(this::loadTimeRangeBasedEntitiesCount));
    EntityRateLimiterCacheConfig globalRateLimiterCacheConfig =
        entityRateLimiterConfig.getGlobalRateLimiterCacheConfig();
    this.globalEntitiesCount =
        CacheBuilder.newBuilder()
            .refreshAfterWrite(globalRateLimiterCacheConfig.getRefreshDuration())
            .expireAfterWrite(globalRateLimiterCacheConfig.getExpiryDuration())
            .maximumSize(globalRateLimiterCacheConfig.getMaxSize())
            .recordStats()
            .build(CacheLoader.from(this::loadGlobalEntitiesCount));

    PlatformMetricsRegistry.registerCache(
        this.getClass().getName() + DOT + "entitiesCount",
        globalEntitiesCount,
        Collections.emptyMap());
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
    List<AttributeFilter> childFilters =
        Lists.newArrayList(
            AttributeFilter.newBuilder()
                .setName("attributes.api_discovery_state")
                .setOperator(Operator.NEQ)
                .setAttributeValue(
                    AttributeValue.newBuilder()
                        .setValue(
                            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                                .setString("MERGED")
                                .build())
                        .build())
                .build());

    addEnvironmentFilter(rateLimitKey, childFilters);
    org.hypertrace.core.documentstore.Query query =
        DocStoreConverter.transform(
            rateLimitKey.getTenantId(),
            org.hypertrace.entity.data.service.v1.Query.newBuilder()
                .setFilter(
                    AttributeFilter.newBuilder()
                        .setOperator(Operator.AND)
                        .addAllChildFilter(childFilters)
                        .build())
                .setEntityType(rateLimitKey.getEntityType())
                .build(),
            Collections.emptyList());

    long total = entitiesCollection.total(query);
    log.info("Global entities count query {} {} {}", rateLimitKey, query, total);
    return total;
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

  private Long loadTimeRangeBasedEntitiesCount(RateLimitKey rateLimitKey) {
    long timestamp =
        System.currentTimeMillis() - entityRateLimiterConfig.getWindowDuration().toMillis();

    List<AttributeFilter> childFilters =
        Lists.newArrayList(
            AttributeFilter.newBuilder()
                .setName("attributes.api_discovery_state")
                .setOperator(Operator.NEQ)
                .setAttributeValue(
                    AttributeValue.newBuilder()
                        .setValue(
                            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                                .setString("MERGED")
                                .build())
                        .build())
                .build(),
            AttributeFilter.newBuilder()
                .setName("createdTime")
                .setOperator(Operator.GT)
                .setAttributeValue(
                    AttributeValue.newBuilder()
                        .setValue(
                            org.hypertrace.entity.data.service.v1.Value.newBuilder()
                                .setLong(timestamp)
                                .build())
                        .build())
                .build());
    addEnvironmentFilter(rateLimitKey, childFilters);

    org.hypertrace.core.documentstore.Query query =
        DocStoreConverter.transform(
            rateLimitKey.getTenantId(),
            org.hypertrace.entity.data.service.v1.Query.newBuilder()
                .setFilter(
                    AttributeFilter.newBuilder()
                        .setOperator(Operator.AND)
                        .addAllChildFilter(childFilters)
                        .build())
                .setEntityType(rateLimitKey.getEntityType())
                .build(),
            Collections.emptyList());

    long total = entitiesCollection.total(query);
    log.info("Time range entities count query {} {} {}", rateLimitKey, query, total);
    return total;
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
          rateLimitConfig.get().getTimeRangeEntitiesLimit() < (windowEntitiesCount + entities.size());
      if (isGlobalEntitiesLimitBreached) {
        log.info("Global limit breached");
      }

      if (isTimeRangeLimitBreached) {
        log.info("Time Range Limit breached");
      }
      return isGlobalEntitiesLimitBreached || isTimeRangeLimitBreached;
    } catch (ExecutionException e) {
      log.error("Error while evaluating rate limits {}", requestContext, e);
    }
    return false;
  }

  private Optional<String> getEnvironment(RequestContext requestContext, Entity entity) {
    Optional<String> environmentAttributeId =
        this.entityRateLimiterConfig.getEnvironmentAttributeId(
        entity.getEntityType());
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
