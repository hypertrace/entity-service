package org.hypertrace.entity.data.service.rxclient;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.reactivex.rxjava3.core.Single;
import java.time.Duration;
import javax.annotation.Nonnull;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc;
import org.hypertrace.entity.data.service.v1.EntityDataServiceGrpc.EntityDataServiceStub;

class EntityDataCachingClient implements EntityDataClient {
  private final EntityDataServiceStub entityDataClient;
  private final LoadingCache<EntityCacheKey, Single<Entity>> cache;

  EntityDataCachingClient(
      @Nonnull Channel channel,
      @Nonnull CallCredentials credentials,
      int maxCacheContexts,
      @Nonnull Duration cacheExpiration) {
    this.entityDataClient = EntityDataServiceGrpc.newStub(channel).withCallCredentials(credentials);
    this.cache =
        CacheBuilder.newBuilder()
            .maximumSize(maxCacheContexts)
            .expireAfterWrite(cacheExpiration)
            .build(CacheLoader.from(this::upsertEntity));
  }

  @Override
  public Single<Entity> getOrCreateEntity(Entity entity) {
    EntityCacheKey cacheKey = EntityCacheKey.entityInCurrentContext(entity);
    return this.cache.getUnchecked(cacheKey).doOnError(x -> this.cache.invalidate(cacheKey));
  }

  private Single<Entity> upsertEntity(EntityCacheKey key) {
    return key.getExecutionContext().<Entity>stream(
            streamObserver -> this.entityDataClient.upsert(key.getInputEntity(), streamObserver))
        .singleOrError()
        .cache();
  }
}
