package org.hypertrace.entity.data.service.rxclient;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.Striped;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.internal.functions.Functions;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceStub;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityResponse;

@Slf4j
class EntityDataCachingClient implements EntityDataClient {
  private static final int PENDING_UPDATE_MAX_LOCK_COUNT = 1000;
  private final EntityDataServiceStub entityDataClient;
  private final LoadingCache<EntityKey, Single<Entity>> cache;
  private final ConcurrentMap<EntityKey, PendingEntityUpdate> pendingEntityUpdates =
      new ConcurrentHashMap<>();
  private final Striped<Lock> pendingUpdateStripedLock =
      Striped.lazyWeakLock(PENDING_UPDATE_MAX_LOCK_COUNT);
  private final Clock clock;

  EntityDataCachingClient(
      Clock clock,
      @Nonnull Channel channel,
      @Nonnull CallCredentials credentials,
      int maxCacheContexts,
      @Nonnull Duration cacheExpiration) {
    this.clock = clock;
    this.entityDataClient = EntityDataServiceGrpc.newStub(channel).withCallCredentials(credentials);
    this.cache =
        CacheBuilder.newBuilder()
            .maximumSize(maxCacheContexts)
            .expireAfterWrite(cacheExpiration)
            .build(CacheLoader.from(this::upsertEntityWithoutCaching));
  }

  @Override
  public void createOrUpdateEntityEventually(
      RequestContext requestContext,
      Entity entity,
      UpsertCondition condition,
      Duration maximumUpsertDelay) {
    EntityKey entityKey = new EntityKey(requestContext, entity);

    // Don't allow other update processing until finished
    Lock lock = this.pendingUpdateStripedLock.get(entityKey);
    try {
      lock.lock();
      this.pendingEntityUpdates
          .computeIfAbsent(entityKey, unused -> new PendingEntityUpdate())
          .addNewUpdate(entityKey, condition, maximumUpsertDelay);
    } finally {
      lock.unlock();
    }
  }

  @Override
  public Single<Entity> createOrUpdateEntity(
      RequestContext requestContext, Entity entity, UpsertCondition upsertCondition) {
    return this.createOrUpdateEntity(new EntityKey(requestContext, entity), upsertCondition);
  }

  private Single<Entity> createOrUpdateEntity(
      EntityKey entityKey, UpsertCondition upsertCondition) {
    Single<Entity> updateResult =
        this.upsertEntityWithoutCaching(entityKey, upsertCondition).cache();
    EntityDataCachingClient.this.cache.put(entityKey, updateResult);
    return updateResult;
  }

  private Single<Entity> upsertEntityWithoutCaching(EntityKey key) {
    return this.upsertEntityWithoutCaching(key, UpsertCondition.getDefaultInstance());
  }

  private Single<Entity> upsertEntityWithoutCaching(EntityKey key, UpsertCondition condition) {
    return key.getExecutionContext().<MergeAndUpsertEntityResponse>stream(
            streamObserver ->
                this.entityDataClient.mergeAndUpsertEntity(
                    MergeAndUpsertEntityRequest.newBuilder()
                        .setEntity(key.getInputEntity())
                        .setUpsertCondition(condition)
                        .build(),
                    streamObserver))
        .map(MergeAndUpsertEntityResponse::getEntity)
        .singleOrError()
        .cache();
  }

  private class PendingEntityUpdate {
    private EntityKey entityKey;
    private Disposable updateExecutionTimer;
    private Instant currentDeadline;
    private UpsertCondition condition;

    private void executeUpdate() {
      // Make sure no current additions
      Lock lock = pendingUpdateStripedLock.get(entityKey);
      try {
        lock.lock();
        EntityDataCachingClient.this.pendingEntityUpdates.remove(entityKey);
      } finally {
        lock.unlock();
      }
      EntityDataCachingClient.this
          .createOrUpdateEntity(entityKey, condition)
          .blockingSubscribe(
              Functions.emptyConsumer(), error -> log.error("Error upserting entity", error));
    }

    private void addNewUpdate(
        EntityKey newEntityKey, UpsertCondition condition, Duration maximumDelay) {
      if (nonNull(updateExecutionTimer) && updateExecutionTimer.isDisposed()) {
        throw new IllegalStateException("Attempting to add new update after execution");
      }

      this.entityKey =
          Optional.ofNullable(this.entityKey)
              .map(existingKey -> existingKey.mergeOtherEntity(newEntityKey.getInputEntity()))
              .orElse(newEntityKey);
      this.condition = condition;
      Instant newDeadline = EntityDataCachingClient.this.clock.instant().plus(maximumDelay);
      if (isNull(currentDeadline) || this.currentDeadline.isAfter(newDeadline)) {
        this.currentDeadline = newDeadline;
        if (nonNull(updateExecutionTimer)) {
          updateExecutionTimer.dispose();
        }

        this.updateExecutionTimer =
            Completable.timer(maximumDelay.toNanos(), TimeUnit.NANOSECONDS)
                .subscribe(this::executeUpdate);
      }
    }
  }
}
