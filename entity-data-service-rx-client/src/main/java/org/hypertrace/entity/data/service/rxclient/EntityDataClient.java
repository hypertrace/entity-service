package org.hypertrace.entity.data.service.rxclient;

import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.reactivex.rxjava3.core.Single;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.data.service.v1.Entity;

/**
 * EntityDataClient is an asynchronous, potentially caching client, keyin entities as described in
 * {@link EntityCacheKey}
 */
public interface EntityDataClient {

  /**
   * Gets the entity from the cache if available, otherwise upserts it and returns the result. The
   * behavior of this may or may not be cached depending on the configuration.
   *
   * @param entity
   * @return
   */
  Single<Entity> getOrCreateEntity(Entity entity);

  static Builder builder(@Nonnull Channel channel) {
    return new Builder(Objects.requireNonNull(channel));
  }

  final class Builder {
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
          this.channel, this.callCredentials, this.maxCacheContexts, this.cacheExpiration);
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
