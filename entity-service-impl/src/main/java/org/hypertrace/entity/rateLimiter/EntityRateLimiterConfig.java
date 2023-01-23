package org.hypertrace.entity.rateLimiter;

import static java.util.stream.Collectors.toUnmodifiableMap;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigRenderOptions;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.hypertrace.entity.data.service.v1.AttributeFilter;
import org.hypertrace.entity.data.service.v1.AttributeFilter.Builder;

@Slf4j
public class EntityRateLimiterConfig {

  private static final JsonFormat.Parser JSON_PARSER = JsonFormat.parser().ignoringUnknownFields();
  private static final ConfigRenderOptions CONFIG_RENDER_CONCISE = ConfigRenderOptions.concise();
  private static final String ALL = "*";
  private static final String ENTITIES_RATE_LIMIT = "entities.rateLimit";
  private static final String WINDOW_SIZE = "windowSize";

  private static final String ENTITIES_RATE_LIMIT_DISABLED = "disabled";

  private static final String GLOBAL_ENTITIES_RATE_LIMIT_CACHE = "cache.global";
  private static final String WINDOW_ENTITIES_RATE_LIMIT_CACHE = "cache.window";
  private static final String REFRESH_DURATION = "refresh";
  private static final String EXPIRY_DURATION = "expiry";
  private static final String MAX_SIZE = "maxSize";

  private static final String ENTITIES_RATE_LIMIT_ENTITY_INFO_ATTRIBUTE_MAP = "entityInfo";
  private static final String ENTITY_TYPE = "entityType";
  private static final String ENVIRONMENT = "environment";
  private static final String DEFAULT_FILTER = "defaultFilter";

  private static final String ENTITIES_RATE_LIMIT_CONFIG = "limitConfig";
  private static final String TENANT = "tenantId";
  private static final String WINDOW_RATE_LIMIT = "limits.window";
  private static final String GLOBAL_RATE_LIMIT = "limits.global";

  private static final Duration DEFAULT_REFRESH_DURATION = Duration.ofMinutes(10);
  private static final Duration DEFAULT_EXPIRY_DURATION = Duration.ofMinutes(60);
  private static final int DEFAULT_MAX_SIZE = 1000;

  private final boolean disabled;
  private final Duration windowDuration;

  private final EntityRateLimiterCacheConfig windowRateLimiterCacheConfig;
  private final EntityRateLimiterCacheConfig globalRateLimiterCacheConfig;
  private final Map<String, String> typeToEnvironmentMap;
  private final Map<String, Optional<AttributeFilter>> typeToDefaultFilterMap;
  private final Map<RateLimitKey, RateLimitConfig> rateLimitConfigMap;

  public EntityRateLimiterConfig(Config config) {
    Config rateLimitConfig = config.getConfig(ENTITIES_RATE_LIMIT);
    this.disabled = rateLimitConfig.getBoolean(ENTITIES_RATE_LIMIT_DISABLED);
    this.windowDuration =
        rateLimitConfig.hasPath(WINDOW_SIZE)
            ? rateLimitConfig.getDuration(WINDOW_SIZE)
            : Duration.ofMinutes(60);

    this.rateLimitConfigMap =
        rateLimitConfig.hasPath(ENTITIES_RATE_LIMIT_CONFIG)
            ? rateLimitConfig.getConfigList(ENTITIES_RATE_LIMIT_CONFIG).stream()
                .collect(
                    toUnmodifiableMap(
                        limitConfig ->
                            new RateLimitKey(
                                limitConfig.getString(TENANT),
                                Optional.ofNullable(limitConfig.getString(ENVIRONMENT)),
                                limitConfig.getString(ENTITY_TYPE)),
                        limitConfig ->
                            new RateLimitConfig(
                                limitConfig.getInt(GLOBAL_RATE_LIMIT),
                                limitConfig.getInt(WINDOW_RATE_LIMIT))))
            : new HashMap<>();

    this.typeToEnvironmentMap =
        rateLimitConfig.hasPath(ENTITIES_RATE_LIMIT_ENTITY_INFO_ATTRIBUTE_MAP)
            ? rateLimitConfig.getConfigList(ENTITIES_RATE_LIMIT_ENTITY_INFO_ATTRIBUTE_MAP).stream()
                .collect(
                    toUnmodifiableMap(
                        attributeConfig -> attributeConfig.getString(ENTITY_TYPE),
                        attributeConfig -> attributeConfig.getString(ENVIRONMENT)))
            : new HashMap<>();

    this.typeToDefaultFilterMap =
        rateLimitConfig.hasPath(ENTITIES_RATE_LIMIT_ENTITY_INFO_ATTRIBUTE_MAP)
            ? rateLimitConfig.getConfigList(ENTITIES_RATE_LIMIT_ENTITY_INFO_ATTRIBUTE_MAP).stream()
                .collect(
                    toUnmodifiableMap(
                        attributeConfig -> attributeConfig.getString(ENTITY_TYPE),
                        attributeConfig -> {
                          if (attributeConfig.hasPath(DEFAULT_FILTER)) {
                            Config attributeFilter = attributeConfig.getConfig(DEFAULT_FILTER);
                            try {
                              Builder builder = AttributeFilter.newBuilder();
                              JSON_PARSER.merge(
                                  attributeFilter.root().render(CONFIG_RENDER_CONCISE), builder);
                              return Optional.of(builder.build());
                            } catch (InvalidProtocolBufferException e) {
                              log.error("Error while parsing filter {}", attributeFilter, e);
                            }
                          }
                          return Optional.empty();
                        }))
            : new HashMap<>();

    Duration refreshDuration =
        rateLimitConfig.hasPath(dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, REFRESH_DURATION))
            ? rateLimitConfig.getDuration(
                dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, REFRESH_DURATION))
            : DEFAULT_REFRESH_DURATION;
    Duration expiryDuration =
        rateLimitConfig.hasPath(dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, EXPIRY_DURATION))
            ? rateLimitConfig.getDuration(
                dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, EXPIRY_DURATION))
            : DEFAULT_EXPIRY_DURATION;
    int maxSize =
        rateLimitConfig.hasPath(dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, MAX_SIZE))
            ? rateLimitConfig.getInt(dotJoiner(GLOBAL_ENTITIES_RATE_LIMIT_CACHE, MAX_SIZE))
            : DEFAULT_MAX_SIZE;
    this.globalRateLimiterCacheConfig =
        new EntityRateLimiterCacheConfig(refreshDuration, expiryDuration, maxSize);

    refreshDuration =
        rateLimitConfig.hasPath(dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, REFRESH_DURATION))
            ? rateLimitConfig.getDuration(
                dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, REFRESH_DURATION))
            : DEFAULT_REFRESH_DURATION;
    expiryDuration =
        rateLimitConfig.hasPath(dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, EXPIRY_DURATION))
            ? rateLimitConfig.getDuration(
                dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, EXPIRY_DURATION))
            : DEFAULT_EXPIRY_DURATION;
    maxSize =
        rateLimitConfig.hasPath(dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, MAX_SIZE))
            ? rateLimitConfig.getInt(dotJoiner(WINDOW_ENTITIES_RATE_LIMIT_CACHE, MAX_SIZE))
            : DEFAULT_MAX_SIZE;
    this.windowRateLimiterCacheConfig =
        new EntityRateLimiterCacheConfig(refreshDuration, expiryDuration, maxSize);
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
    return Optional.ofNullable(typeToEnvironmentMap.get(entityType));
  }

  public Optional<AttributeFilter> getDefaultAttributeFilter(String entityType) {
    return typeToDefaultFilterMap.get(entityType);
  }

  public Optional<RateLimitConfig> getRateLimitConfig(
      String tenantId, Optional<String> environment, String entityType) {
    RateLimitConfig rateLimitConfig =
        rateLimitConfigMap.get(new RateLimitKey(tenantId, environment, entityType));

    if (rateLimitConfig == null) {
      rateLimitConfig =
          rateLimitConfigMap.get(new RateLimitKey(tenantId, Optional.of(ALL), entityType));
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
