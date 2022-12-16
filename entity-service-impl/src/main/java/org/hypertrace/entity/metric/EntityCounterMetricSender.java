package org.hypertrace.entity.metric;

import io.micrometer.core.instrument.Counter;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.change.event.impl.ChangeResult;
import org.hypertrace.entity.service.change.event.impl.EntityChangeEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityCounterMetricSender {

  private static final Logger LOGGER = LoggerFactory.getLogger(EntityCounterMetricSender.class);
  private static final String ENTITIES_CREATE_COUNTER = "entities.create.counter";
  private static final String ENTITIES_UPDATE_COUNTER = "entities.update.counter";
  private static final String ENTITIES_DELETE_COUNTER = "entities.delete.counter";
  private static final String ENTITY_TYPE_TAG = "entityType";
  private static final String TENANT_ID_TAG = "tenantId";
  private static final Map<EntityMetricIdentifier, Counter> typeToTenantEntityCounter =
      new ConcurrentHashMap<>();

  public void sendEntitiesDeleteMetrics(
      RequestContext requestContext, String entityType, Collection<Entity> entities) {
    this.getDeleteCounter(requestContext, entityType).increment(entities.size());
  }

  public void sendEntitiesMetrics(
      RequestContext requestContext,
      Collection<Entity> existingEntities,
      Collection<Entity> updatedEntities) {
    ChangeResult changeResult =
        EntityChangeEvaluator.evaluateChange(existingEntities, updatedEntities);
    changeResult.getCreatedEntity().stream()
        .collect(Collectors.groupingBy(Entity::getEntityType))
        .forEach(
            (entityType, entities) -> {
              this.getCreateCounter(requestContext, entityType).increment(entities.size());
            });
    changeResult.getDeletedEntity().stream()
        .collect(Collectors.groupingBy(Entity::getEntityType))
        .forEach(
            (entityType, entities) -> {
              this.getDeleteCounter(requestContext, entityType).increment(entities.size());
            });
    changeResult
        .getExistingToUpdatedEntitiesMap()
        .forEach(
            (existingEntity, updatedEntity) -> {
              this.getUpdateCounter(requestContext, existingEntity.getEntityType()).increment();
            });
  }

  public void sendEntitiesMetrics(
      RequestContext requestContext,
      String entityType,
      Collection<Entity> existingEntities,
      Collection<Entity> updatedEntities) {
    ChangeResult changeResult =
        EntityChangeEvaluator.evaluateChange(existingEntities, updatedEntities);
    this.getCreateCounter(requestContext, entityType)
        .increment(changeResult.getCreatedEntity().size());
    this.getUpdateCounter(requestContext, entityType)
        .increment(changeResult.getExistingToUpdatedEntitiesMap().size());
    this.getDeleteCounter(requestContext, entityType)
        .increment(changeResult.getDeletedEntity().size());
  }

  private Counter getCreateCounter(RequestContext requestContext, String entityType) {
    String tenantId = requestContext.getTenantId().orElseThrow();
    return getCounter(new EntityMetricIdentifier(tenantId, entityType, ENTITIES_CREATE_COUNTER));
  }

  private Counter getUpdateCounter(RequestContext requestContext, String entityType) {
    String tenantId = requestContext.getTenantId().orElseThrow();
    return getCounter(new EntityMetricIdentifier(tenantId, entityType, ENTITIES_UPDATE_COUNTER));
  }

  private Counter getDeleteCounter(RequestContext requestContext, String entityType) {
    String tenantId = requestContext.getTenantId().orElseThrow();
    return getCounter(new EntityMetricIdentifier(tenantId, entityType, ENTITIES_DELETE_COUNTER));
  }

  private Counter getCounter(EntityMetricIdentifier entityMetricIdentifier) {
    return typeToTenantEntityCounter.computeIfAbsent(
        entityMetricIdentifier,
        metricIdentifier ->
            PlatformMetricsRegistry.registerCounter(
                metricIdentifier.getMetricType(),
                Map.of(
                    ENTITY_TYPE_TAG,
                    metricIdentifier.getEntityType(),
                    TENANT_ID_TAG,
                    metricIdentifier.getTenantId())));
  }

  @AllArgsConstructor
  @Value
  private static class EntityMetricIdentifier {

    String tenantId;
    String entityType;
    String metricType;
  }
}
