package org.hypertrace.entity.data.service.rxclient;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.core.SingleObserver;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.subjects.SingleSubject;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceStub;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EntityDataCachingClient implements EntityDataClient {
  private static final Logger LOG = LoggerFactory.getLogger(EntityDataCachingClient.class);
  private final EntityDataServiceStub entityDataClient;
  private final LoadingCache<EntityKey, Single<Entity>> cache;
  private final ConcurrentMap<EntityKey, PendingEntityUpdate> pendingEntityUpdates =
      new ConcurrentHashMap<>();
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
  public Single<Entity> getOrCreateEntity(Entity entity) {
    EntityKey entityKey = EntityKey.entityInCurrentContext(entity);
    return this.cache.getUnchecked(entityKey).doOnError(x -> this.cache.invalidate(entityKey));
  }

  @Override
  public Single<Entity> createOrUpdateEntityEventually(
      Entity entity, UpsertCondition condition, Duration maximumUpsertDelay) {
    SingleSubject<Entity> singleSubject = SingleSubject.create();
    EntityKey entityKey = EntityKey.entityInCurrentContext(entity);
    if (this.pendingEntityUpdates.containsKey(entityKey)) {
      // Update the key - not strictly necessary (because the pending update holds the write data),
      // but good hygiene
      this.pendingEntityUpdates.put(entityKey, this.pendingEntityUpdates.get(entityKey));
    }
    this.pendingEntityUpdates
        .computeIfAbsent(entityKey, unused -> new PendingEntityUpdate())
        .addNewUpdate(entityKey, singleSubject, condition, maximumUpsertDelay);
    return singleSubject;
  }

  @Override
  public Single<Entity> createOrUpdateEntity(Entity entity, UpsertCondition upsertCondition) {
    return this.createOrUpdateEntity(EntityKey.entityInCurrentContext(entity), upsertCondition);
  }

  private Single<Entity> createOrUpdateEntity(EntityKey entityKey, UpsertCondition upsertCondition) {
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
    private final List<SingleObserver<Entity>> responseObservers = new LinkedList<>();
    private EntityKey entityKey;
    private Disposable updateExecutionTimer;
    private Instant currentDeadline;
    private UpsertCondition condition;

    private void executeUpdate() {
      LOG.warn(
          "Executing update for {} at {}", entityKey.getInputEntity().getEntityId(), Instant.now());

      EntityDataCachingClient.this.pendingEntityUpdates.remove(entityKey);

      Single<Entity> updateResult =
          EntityDataCachingClient.this.createOrUpdateEntity(entityKey, condition).cache();
      EntityDataCachingClient.this.cache.put(entityKey, updateResult);

      responseObservers.forEach(updateResult::subscribe);
    }

    private void addNewUpdate(
        EntityKey entityKey,
        SingleObserver<Entity> observer,
        UpsertCondition condition,
        Duration maximumDelay) {
      this.entityKey = entityKey;
      this.condition = condition;
      this.responseObservers.add(observer);
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
