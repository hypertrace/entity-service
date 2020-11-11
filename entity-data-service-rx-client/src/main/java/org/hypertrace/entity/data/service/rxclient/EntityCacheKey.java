package org.hypertrace.entity.data.service.rxclient;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Equivalence;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.hypertrace.core.grpcutils.client.rx.GrpcRxExecutionContext;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.v1.entitytype.EntityType;

/**
 * Caches based on the tenant id, entity type and entity id if available. If entity id is not
 * available, the key falls back to an equality comparison of the identifying attributes map.
 */
class EntityCacheKey {

  private static final String DEFAULT_TENANT_ID = "default";

  static EntityCacheKey entityInCurrentContext(Entity inputEntity) {
    return new EntityCacheKey(
        requireNonNull(RequestContext.CURRENT.get()), requireNonNull(inputEntity));
  }

  private final Entity inputEntity;
  private final String tenantId;
  private final GrpcRxExecutionContext executionContext;
  private final Equivalence<Entity> ENTITY_EQUIVALENCE =
      Equivalence.equals()
          .onResultOf(
              entity ->
                  requireNonNull(entity).getEntityId().isEmpty()
                      ? entity.getIdentifyingAttributesMap()
                      : entity.getEntityId());

  protected EntityCacheKey(@Nonnull RequestContext requestContext, @Nonnull Entity inputEntity) {
    requireNonNull(inputEntity.getEntityId());
    requireNonNull(inputEntity.getEntityType());
    this.executionContext = GrpcRxExecutionContext.forContext(requestContext);
    this.tenantId = requestContext.getTenantId().orElse(DEFAULT_TENANT_ID);
    this.inputEntity = inputEntity;
  }

  public GrpcRxExecutionContext getExecutionContext() {
    return executionContext;
  }

  public Entity getInputEntity() {
    return inputEntity;
  }

  private String getEntityType() {
    return this.getInputEntity().getEntityType();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EntityCacheKey that = (EntityCacheKey) o;
    return tenantId.equals(that.tenantId)
        && getEntityType().equals(that.getEntityType())
        && ENTITY_EQUIVALENCE.equivalent(getInputEntity(), that.getInputEntity());
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId, getEntityType(), ENTITY_EQUIVALENCE.hash(getInputEntity()));
  }
}
