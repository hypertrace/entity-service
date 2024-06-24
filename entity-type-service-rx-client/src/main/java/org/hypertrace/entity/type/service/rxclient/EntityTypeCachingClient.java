package org.hypertrace.entity.type.service.rxclient;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.hypertrace.entity.type.service.v2.EntityType;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceGrpc;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceGrpc.EntityTypeServiceStub;
import org.hypertrace.entity.type.service.v2.QueryEntityTypesRequest;
import org.hypertrace.entity.type.service.v2.QueryEntityTypesResponse;

class EntityTypeCachingClient implements EntityTypeClient {

  private final EntityTypeServiceStub entityTypeClient;
  private final LoadingCache<TenantBasedCacheKey, Single<Map<String, EntityType>>> cache;

  EntityTypeCachingClient(
      @Nonnull Channel channel,
      @Nonnull CallCredentials credentials,
      int maxCacheContexts,
      @Nonnull Duration cacheExpiration) {
    this.entityTypeClient = EntityTypeServiceGrpc.newStub(channel).withCallCredentials(credentials);
    this.cache =
        CacheBuilder.newBuilder()
            .maximumSize(maxCacheContexts)
            .expireAfterWrite(cacheExpiration)
            .build(CacheLoader.from(this::fetchTypes));
  }

  @Override
  public Observable<EntityType> getAll() {
    return this.getOrInvalidate(TenantBasedCacheKey.forCurrentContext())
        .flattenAsObservable(Map::values);
  }

  @Override
  public Single<EntityType> get(String name) {
    return this.getOrInvalidate(TenantBasedCacheKey.forCurrentContext())
        .mapOptional(map -> Optional.ofNullable(map.get(name)))
        .switchIfEmpty(Single.error(this.buildErrorForMissingType(name)));
  }

  private Single<Map<String, EntityType>> fetchTypes(TenantBasedCacheKey key) {
    return key.getExecutionContext().<QueryEntityTypesResponse>stream(
            streamObserver ->
                this.entityTypeClient
                    .withDeadlineAfter(10, TimeUnit.SECONDS)
                    .queryEntityTypes(QueryEntityTypesRequest.getDefaultInstance(), streamObserver))
        .flatMapIterable(QueryEntityTypesResponse::getEntityTypeList)
        .toMap(EntityType::getName)
        .map(Collections::unmodifiableMap)
        .cache();
  }

  private Single<Map<String, EntityType>> getOrInvalidate(TenantBasedCacheKey key) {
    return this.cache.getUnchecked(key).doOnError(x -> this.cache.invalidate(key));
  }

  private NoSuchElementException buildErrorForMissingType(String name) {
    return new NoSuchElementException(
        String.format("No entity type available for name '%s'", name));
  }
}
