package org.hypertrace.entity.rateLimiter;

import static java.util.stream.Collectors.toUnmodifiableMap;

import com.typesafe.config.Config;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class EntityRateLimiterConfig {

  private static final String ALL = "*";
  private static final String ENTITIES_RATE_LIMIT = "entities.rateLimit";
  private static final String ENTITIES_RATE_LIMIT_DISABLED = "entities.rateLimit.disabled";
  private static final String ENTITIES_RATE_LIMIT_CONFIG = "entities.rateLimit.config";
  private static final String GLOBAL_ENTITIES_RATE_LIMIT_CACHE = "entities.rateLimit.globalRateLimiter.cache";
  private static final String WINDOW_ENTITIES_RATE_LIMIT_CACHE = "entities.rateLimit.windowRateLimiter.cache";

  private static final String WINDOW_RATE_LIMITER = "windowRateLimiter";
  private static final String WINDOW = "window";
  private static final String REFRESH_DURATION = "refresh";
  private static final String EXPIRY_DURATION = "expiry";
  private static final String MAX_SIZE = "size";

  private static final String ENTITIES_RATE_LIMIT_ENVIRONMENT_ATTRIBUTE_MAP = "environmentMap";
  private static final String ENTITY_TYPE = "entityType";
  private static final String ENVIRONMENT = "environment";
  private static final String TENANT = "tenantId";
  private static final String WINDOW_RATE_LIMIT = "windowRateLimit";
  private static final String GLOBAL_RATE_LIMIT = "globalRateLimit";

  private static final Duration DEFAULT_REFRESH_DURATION = Duration.ofMinutes(10);
  private static final Duration DEFAULT_EXPIRY_DURATION = Duration.ofMinutes(60);
  private static final int DEFAULT_MAX_SIZE = 1000;

  private final boolean disabled;
  private final Duration windowDuration;

  private final EntityRateLimiterCacheConfig windowRateLimiterCacheConfig;
  private final EntityRateLimiterCacheConfig globalRateLimiterCacheConfig;
  private final Map<String, String> entityTypeToEnvironmentMap;
  private final Map<RateLimitKey, RateLimitConfig> rateLimitConfigMap;

  public EntityRateLimiterConfig(Config config) {
    this.disabled = config.getBoolean(ENTITIES_RATE_LIMIT_DISABLED);
    this.windowDuration =
        config.hasPath(dotJoiner(ENTITIES_RATE_LIMIT, WINDOW_RATE_LIMITER, WINDOW))
            ? config.getDuration(dotJoiner(ENTITIES_RATE_LIMIT, WINDOW_RATE_LIMITER, WINDOW))
            : Duration.ofMinutes(60);

    this.rateLimitConfigMap =
        config.hasPath(ENTITIES_RATE_LIMIT_CONFIG)
            ? config.getConfigList(ENTITIES_RATE_LIMIT_CONFIG).stream()
            .collect(
                toUnmodifiableMap(
                    rateLimitConfig ->
                        new RateLimitKey(
                            rateLimitConfig.getString(TENANT),
                            Optional.ofNullable(rateLimitConfig.getString(ENVIRONMENT)),
                            rateLimitConfig.getString(ENTITY_TYPE)),
                    rateLimitConfig ->
                        new RateLimitConfig(
                            rateLimitConfig.getInt(GLOBAL_RATE_LIMIT),
                            rateLimitConfig.getInt(WINDOW_RATE_LIMIT))))
            : new HashMap<>();

    this.entityTypeToEnvironmentMap =
        config.hasPath(ENTITIES_RATE_LIMIT_ENVIRONMENT_ATTRIBUTE_MAP)
            ? config
            .getConfigList(ENTITIES_RATE_LIMIT_ENVIRONMENT_ATTRIBUTE_MAP)
            .stream()
            .collect(
                toUnmodifiableMap(
                    attributeConfig -> attributeConfig.getString(ENTITY_TYPE),
                    attributeConfig -> attributeConfig.getString(ENVIRONMENT)))
            : new HashMap<>();

    Duration refreshDuration =
        config.hasPath(
            dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, REFRESH_DURATION))
            ? config.getDuration(
            dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, REFRESH_DURATION))
            : DEFAULT_REFRESH_DURATION;
    Duration expiryDuration =
        config.hasPath(dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, EXPIRY_DURATION))
            ? config.getDuration(
            dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, EXPIRY_DURATION))
            : DEFAULT_EXPIRY_DURATION;
    int maxSize =
        config.hasPath(dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, MAX_SIZE))
            ? config.getInt(dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, MAX_SIZE))
            : DEFAULT_MAX_SIZE;
    this.globalRateLimiterCacheConfig = new EntityRateLimiterCacheConfig(refreshDuration,
        expiryDuration, maxSize);

    refreshDuration =
        config.hasPath(dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, REFRESH_DURATION))
            ? config.getDuration(
            dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, REFRESH_DURATION))
            : DEFAULT_REFRESH_DURATION;
    expiryDuration =
        config.hasPath(dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, EXPIRY_DURATION))
            ? config.getDuration(
            dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, EXPIRY_DURATION))
            : DEFAULT_EXPIRY_DURATION;
    maxSize =
        config.hasPath(dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, MAX_SIZE))
            ? config.getInt(dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, MAX_SIZE))
            : DEFAULT_MAX_SIZE;
    this.windowRateLimiterCacheConfig = new EntityRateLimiterCacheConfig(refreshDuration,
        expiryDuration, maxSize);
  }

  private String dotJoiner(String... strings) {
    return String.join(".", strings);
  }

  public Duration getWindowDuration() {
    return this.windowDuration;
  }

  public boolean isDisabled() {
    return disabled;
  }

  public EntityRateLimiterCacheConfig getGlobalRateLimiterCacheConfig() {
    return this.globalRateLimiterCacheConfig;
  }

  public EntityRateLimiterCacheConfig getWindowRateLimiterCacheConfig() {
    return this.windowRateLimiterCacheConfig;
  }

  public Optional<String> getEnvironmentAttributeId(String entityType) {
    return Optional.ofNullable(entityTypeToEnvironmentMap.get(entityType));
  }

  public Optional<RateLimitConfig> getRateLimitConfig(
      String tenantId, Optional<String> environment, String entityType) {
    RateLimitConfig rateLimitConfig =
        rateLimitConfigMap.get(new RateLimitKey(tenantId, environment, entityType));

    if (rateLimitConfig == null) {
      rateLimitConfig = rateLimitConfigMap.get(
          new RateLimitKey(tenantId, Optional.of(ALL), entityType));
    }

    if (rateLimitConfig == null) {
      rateLimitConfig = rateLimitConfigMap.get(new RateLimitKey(ALL, Optional.of(ALL), entityType));
    }

    log.info(
        "Rate limit config is {} {} {} {} ", tenantId, environment, entityType, rateLimitConfig);
    return Optional.ofNullable(rateLimitConfig);
  }

  @Value
  public static class RateLimitConfig {

    int globalEntitiesLimit;
    int timeRangeEntitiesLimit;
  }
}
