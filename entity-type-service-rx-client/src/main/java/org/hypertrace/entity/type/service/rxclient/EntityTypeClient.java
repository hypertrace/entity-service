package org.hypertrace.entity.type.service.rxclient;

import io.grpc.CallCredentials;
import io.grpc.Channel;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Objects;
import javax.annotation.Nonnull;
import org.hypertrace.core.grpcutils.client.RequestContextClientCallCredsProviderFactory;
import org.hypertrace.entity.type.service.v2.EntityType;

/** An asynchronous configurable type client */
public interface EntityTypeClient {

  Observable<EntityType> getAll();

  Single<EntityType> get(String name);

  static Builder builder(@Nonnull Channel channel) {
    return new Builder(Objects.requireNonNull(channel));
  }

  final class Builder {
    private final Channel channel;
    private int maxCacheContexts = 10000;
    private Duration cacheExpiration = Duration.of(15, ChronoUnit.MINUTES);
    private CallCredentials callCredentials =
        RequestContextClientCallCredsProviderFactory.getClientCallCredsProvider().get();

    private Builder(Channel channel) {
      this.channel = channel;
    }

    public EntityTypeClient build() {
      return new EntityTypeCachingClient(
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
