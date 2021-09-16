package org.hypertrace.entity.data.service.rxclient;

import static java.util.Objects.requireNonNull;

import com.google.common.base.Equivalence;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.hypertrace.core.grpcutils.client.rx.GrpcRxExecutionContext;
import org.hypertrace.core.grpcutils.context.ContextualKey;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;

/**
 * Caches based on the tenant id, entity type and entity id if available. If entity id is not
 * available, the key falls back to an equality comparison of the identifying attributes map.
 */
class EntityKey {
  private final Entity inputEntity;
  private final ContextualKey<Void> contextualKey;
  private final RequestContext requestContext;
  private static final Equivalence<Entity> ENTITY_EQUIVALENCE =
      Equivalence.equals()
          .onResultOf(
              entity ->
                  requireNonNull(entity).getEntityId().isEmpty()
                      ? entity.getIdentifyingAttributesMap()
                      : entity.getEntityId());

  EntityKey(@Nonnull RequestContext requestContext, @Nonnull Entity inputEntity) {
    requireNonNull(inputEntity.getEntityId());
    requireNonNull(inputEntity.getEntityType());
    this.contextualKey = requestContext.buildContextualKey();
    this.requestContext = requestContext;
    this.inputEntity = inputEntity;
  }

  public GrpcRxExecutionContext getExecutionContext() {
    return GrpcRxExecutionContext.forContext(requestContext);
  }

  public Entity getInputEntity() {
    return inputEntity;
  }

  private String getEntityType() {
    return this.getInputEntity().getEntityType();
  }

  public EntityKey mergeOtherEntity(Entity otherEntity) {
    return new EntityKey(
        this.requestContext, this.inputEntity.toBuilder().mergeFrom(otherEntity).build());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    EntityKey that = (EntityKey) o;
    return contextualKey.equals(that.contextualKey)
        && getEntityType().equals(that.getEntityType())
        && ENTITY_EQUIVALENCE.equivalent(getInputEntity(), that.getInputEntity());
  }

  @Override
  public int hashCode() {
    return Objects.hash(contextualKey, getEntityType(), ENTITY_EQUIVALENCE.hash(getInputEntity()));
  }
}
