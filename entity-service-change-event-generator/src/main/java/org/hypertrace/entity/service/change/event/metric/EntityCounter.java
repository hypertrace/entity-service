package org.hypertrace.entity.service.change.event.metric;

import io.micrometer.core.instrument.Counter;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import lombok.AllArgsConstructor;
import org.hypertrace.core.serviceframework.metrics.PlatformMetricsRegistry;

public class EntityCounter {

  private static final String ENTITIES_CREATE_COUNTER = "entities.create.counter";
  private static final String ENTITIES_UPDATE_COUNTER = "entities.update.counter";
  private static final String ENTITIES_DELETE_COUNTER = "entities.delete.counter";
  private static final String ENTITY_TYPE_TAG = "entityType";
  private static final String TENANT_ID_TAG = "tenantId";
  private static final Map<EntityTypeTenantCounterPair, Counter> typeToTenantEntityCounter =
      new ConcurrentHashMap<>();

  private static final EntityCounter entityCounter = new EntityCounter();

  // private constructor
  private EntityCounter() {}

  public static EntityCounter getEntityCounter() {
    return entityCounter;
  }

  public Counter getCreateCounter(String apiType, String tenantId) {
    EntityTypeTenantCounterPair typeTenantPair =
        new EntityTypeTenantCounterPair(apiType, tenantId, ENTITIES_CREATE_COUNTER);
    return typeToTenantEntityCounter.computeIfAbsent(
        typeTenantPair,
        typeTenantPair1 ->
            PlatformMetricsRegistry.registerCounter(
                ENTITIES_CREATE_COUNTER,
                Map.of(ENTITY_TYPE_TAG, apiType, TENANT_ID_TAG, tenantId)));
  }

  public Counter getUpdateCounter(String apiType, String tenantId) {
    EntityTypeTenantCounterPair typeTenantPair =
        new EntityTypeTenantCounterPair(apiType, tenantId, ENTITIES_UPDATE_COUNTER);
    return typeToTenantEntityCounter.computeIfAbsent(
        typeTenantPair,
        typeTenantPair1 ->
            PlatformMetricsRegistry.registerCounter(
                ENTITIES_UPDATE_COUNTER,
                Map.of(ENTITY_TYPE_TAG, apiType, TENANT_ID_TAG, tenantId)));
  }

  public Counter getDeleteCounter(String apiType, String tenantId) {
    EntityTypeTenantCounterPair typeTenantPair =
        new EntityTypeTenantCounterPair(apiType, tenantId, ENTITIES_DELETE_COUNTER);
    return typeToTenantEntityCounter.computeIfAbsent(
        typeTenantPair,
        typeTenantPair1 ->
            PlatformMetricsRegistry.registerCounter(
                ENTITIES_DELETE_COUNTER,
                Map.of(ENTITY_TYPE_TAG, apiType, TENANT_ID_TAG, tenantId)));
  }

  @AllArgsConstructor
  private static class EntityTypeTenantCounterPair {

    private final String entityType;
    private final String tenantId;
    private final String counterType;

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      EntityTypeTenantCounterPair that = (EntityTypeTenantCounterPair) o;
      return entityType.equals(that.entityType)
          && tenantId.equals(that.tenantId)
          && counterType.equals(that.counterType);
    }

    @Override
    public int hashCode() {
      return Objects.hash(entityType, tenantId, counterType);
    }
  }
}
