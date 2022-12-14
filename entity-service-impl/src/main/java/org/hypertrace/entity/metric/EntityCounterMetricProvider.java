package org.hypertrace.entity.metric;

import io.micrometer.core.instrument.Counter;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;
import org.hypertrace.entity.service.change.event.impl.ChangeResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityCounterMetricProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(EntityCounterMetricProvider.class);
  private static final String EMPTY_STRING = "";
  private static final String ENTITIES_CREATE_COUNTER = "entities.create.counter";
  private static final String ENTITIES_UPDATE_COUNTER = "entities.update.counter";
  private static final String ENTITIES_DELETE_COUNTER = "entities.delete.counter";
  private static final String ENTITY_TYPE_TAG = "entityType";
  private static final String TENANT_ID_TAG = "tenantId";
  private static final Map<EntityTypeTenantMetricPair, Counter> typeToTenantEntityCounter =
      new ConcurrentHashMap<>();

  public void sendEntitiesMetrics(
      RequestContext requestContext, String entityType, ChangeResult changeResult) {
    if (entityType == null) {
      LOGGER.warn("Entity type is not defined while sending metrics {}", changeResult);
      entityType = EMPTY_STRING;
    }
    this.getCreateCounter(requestContext, entityType)
        .increment(changeResult.getCreatedEntity().size());
    this.getUpdateCounter(requestContext, entityType)
        .increment(changeResult.getExistingToUpdatedEntitiesMap().size());
    this.getDeleteCounter(requestContext, entityType)
        .increment(changeResult.getDeletedEntity().size());
  }

  private Counter getCreateCounter(RequestContext requestContext, String entityType) {
    String tenantId = requestContext.getTenantId().orElseThrow();
    EntityTypeTenantMetricPair typeTenantPair =
        new EntityTypeTenantMetricPair(tenantId, entityType, ENTITIES_CREATE_COUNTER);
    return typeToTenantEntityCounter.computeIfAbsent(
        typeTenantPair,
        typeTenantPair1 ->
            PlatformMetricsRegistry.registerCounter(
                ENTITIES_CREATE_COUNTER,
                Map.of(ENTITY_TYPE_TAG, entityType, TENANT_ID_TAG, tenantId)));
  }

  private Counter getUpdateCounter(RequestContext requestContext, String entityType) {
    String tenantId = requestContext.getTenantId().orElseThrow();
    EntityTypeTenantMetricPair typeTenantPair =
        new EntityTypeTenantMetricPair(tenantId, entityType, ENTITIES_UPDATE_COUNTER);
    return typeToTenantEntityCounter.computeIfAbsent(
        typeTenantPair,
        typeTenantPair1 ->
            PlatformMetricsRegistry.registerCounter(
                ENTITIES_UPDATE_COUNTER,
                Map.of(ENTITY_TYPE_TAG, entityType, TENANT_ID_TAG, tenantId)));
  }

  private Counter getDeleteCounter(RequestContext requestContext, String entityType) {
    String tenantId = requestContext.getTenantId().orElseThrow();
    EntityTypeTenantMetricPair typeTenantPair =
        new EntityTypeTenantMetricPair(tenantId, entityType, ENTITIES_DELETE_COUNTER);
    return typeToTenantEntityCounter.computeIfAbsent(
        typeTenantPair,
        typeTenantPair1 ->
            PlatformMetricsRegistry.registerCounter(
                ENTITIES_DELETE_COUNTER,
                Map.of(ENTITY_TYPE_TAG, entityType, TENANT_ID_TAG, tenantId)));
  }

  @AllArgsConstructor
  private static class EntityTypeTenantMetricPair {

    private final String tenantId;
    private final String entityType;
    private final String metricType;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EntityTypeTenantMetricPair that = (EntityTypeTenantMetricPair) o;
      return entityType.equals(that.entityType)
          && tenantId.equals(that.tenantId)
          && metricType.equals(that.metricType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(entityType, tenantId, metricType);
    }
  }
}
