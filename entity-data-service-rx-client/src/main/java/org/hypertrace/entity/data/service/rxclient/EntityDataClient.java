package org.hypertrace.entity.data.service.rxclient;

import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.reactivex.rxjava3.core.Single;
import java.time.Clock;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.MergeAndUpsertEntityRequest.UpsertCondition;

/**
 * EntityDataClient is an asynchronous, potentially caching client, keyin entities as described in
 * {@link EntityKey}
 */
public interface EntityDataClient {

  /**
   * Gets the entity from the cache if available, otherwise upserts it and returns the result. The
   * behavior of this may or may not be cached depending on the configuration.
   *
   * @param entity
   * @return
   */
  @Deprecated
  Single<Entity> getOrCreateEntity(Entity entity);

  /**
   * Performs a throttled update of the provided entity, starting after no longer than the provided
   * maximumUpsertDelay. If newer candidates for the same entity are received before this update
   * occurs, the value of the newest value (and its condition, if any) will be used instead. This
   * allows a high number of potentially repetitive entity upserts to be processed without creating
   * excessive overhead. Each returned single will propagate the result of the server call that
   * eventually satisfies its deadline.
   *
   * <p>Example:
   *
   * <ol>
   *   <li>entity-1.v1 arrives at t=0ms with a max delay of 500ms
   *   <li>entity-1.v2 arrives at t=100ms with a max delay of 300ms
   *   <li>entity-2.v1 arrives at t=200ms with a max delay of 300ms
   *   <li>entity-1.v3 arrives at t=300ms with a max delay of 500ms
   * </ol>
   *
   * At t=400ms (the deadline for the second invocation) entity-1 is upserted, using the most recent
   * values (the fourth invocation - entity-1.v3). When this returns, the result will be given to
   * the first, second and fourth invocations (the ones for that entity). At t=500ms, the deadline
   * for the third invocation entity-2 is upserted and returned to the third invocation.
   *
   * @param entity
   * @param upsertCondition
   * @param maximumUpsertDelay
   * @return
   */
  Single<Entity> createOrUpdateEntityEventually(
      Entity entity, UpsertCondition upsertCondition, Duration maximumUpsertDelay);

  /**
   * Immediately creates or updates, merging with any existing data, the provided entity if the
   * provided condition is met. The new value is returned if the condition is met, else if the
   * condition is not met, the existing value is instead returned.
   *
   * @param entity
   * @param upsertCondition
   * @return The resulting entity
   */
  Single<Entity> createOrUpdateEntity(Entity entity, UpsertCondition upsertCondition);

  static Builder builder(@Nonnull Channel channel) {
    return new Builder(Objects.requireNonNull(channel));
  }

  final class Builder {
    private final Clock clock = Clock.systemUTC();
    private final Channel channel;
    private int maxCacheContexts = 1000;
    private Duration cacheExpiration = Duration.of(15, ChronoUnit.MINUTES);
    private CallCredentials callCredentials =
        RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get();

    private Builder(Channel channel) {
      this.channel = channel;
    }

    public EntityDataClient build() {
      return new EntityDataCachingClient(
          this.clock,
          this.channel,
          this.callCredentials,
          this.maxCacheContexts,
          this.cacheExpiration);
    }

    /**
     * Limits the number unique contexts (i.e. tenants) to maintain in the cache at any one time.
     * Defaults to 100.
     *
     * @param maxCacheContexts
     * @return
     */
    public Builder withMaximumCacheContexts(int maxCacheContexts) {
      this.maxCacheContexts = maxCacheContexts;
      return this;
    }

    /**
     * Expires a cached context the provided duration after write. Defaults to 15 minutes.
     *
     * @param cacheExpiration
     * @return
     */
    public Builder withCacheExpiration(@Nonnull Duration cacheExpiration) {
      this.cacheExpiration = cacheExpiration;
      return this;
    }

    /**
     * Use the provided call credentials for propagating context. Defaults to the value provided by
     * {@link RequestContextClientCallCredsProviderFactory}
     *
     * @param callCredentials
     * @return
     */
    public Builder withCallCredentials(@Nonnull CallCredentials callCredentials) {
      this.callCredentials = callCredentials;
      return this;
    }
  }
}
