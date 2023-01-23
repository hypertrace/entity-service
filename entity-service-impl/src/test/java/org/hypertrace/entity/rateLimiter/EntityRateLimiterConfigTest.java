package org.hypertrace.entity.rateLimiter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.Optional;
import org.hypertrace.entity.data.service.v1.AttributeFilter;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Operator;
import org.hypertrace.entity.data.service.v1.Value;
import org.hypertrace.entity.rateLimiter.EntityRateLimiterConfig.RateLimitConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EntityRateLimiterConfigTest {
  @Test
  void testEntityRateLimiterConfig() throws URISyntaxException {
    URI uri =
        getClass().getClassLoader().getResource("rateLimiter/rate_limiter_config.conf").toURI();
    Config config = ConfigFactory.parseFile(new File(uri.getPath()));

    EntityRateLimiterConfig entityRateLimiterConfig = new EntityRateLimiterConfig(config);

    Assertions.assertFalse(entityRateLimiterConfig.isDisabled());

    Assertions.assertEquals(
        entityRateLimiterConfig.getDefaultAttributeFilter("API_TYPE"),
        Optional.of(
            AttributeFilter.newBuilder()
                .setName("attribute_id")
                .setAttributeValue(
                    AttributeValue.newBuilder()
                        .setValue(Value.newBuilder().setString("attr_val").build())
                        .build())
                .setOperator(Operator.EQ)
                .build()));

    Assertions.assertEquals(
        entityRateLimiterConfig.getEnvironmentAttributeId("API_TYPE"),
        Optional.of("API_TYPE.environment"));

    Assertions.assertEquals(entityRateLimiterConfig.getWindowDuration(), Duration.ofMinutes(6));
    Assertions.assertEquals(
        entityRateLimiterConfig.getGlobalRateLimiterCacheConfig(),
        new EntityRateLimiterCacheConfig(Duration.ofMinutes(1), Duration.ofMinutes(6), 100));
    Assertions.assertEquals(
        entityRateLimiterConfig.getWindowRateLimiterCacheConfig(),
        new EntityRateLimiterCacheConfig(Duration.ofMinutes(1), Duration.ofMinutes(6), 100));

    // test rate limit config
    Optional<RateLimitConfig> rateLimitConfig =
        entityRateLimiterConfig.getRateLimitConfig("t", Optional.empty(), "API_TYPE");
    Assertions.assertTrue(rateLimitConfig.isPresent());
    Assertions.assertEquals(rateLimitConfig.get(), new RateLimitConfig(50000, 2000));

    rateLimitConfig =
        entityRateLimiterConfig.getRateLimitConfig("tenantId", Optional.empty(), "API_TYPE");
    Assertions.assertTrue(rateLimitConfig.isPresent());
    Assertions.assertEquals(rateLimitConfig.get(), new RateLimitConfig(50000, 2000));

    rateLimitConfig =
        entityRateLimiterConfig.getRateLimitConfig("tenantId", Optional.of("envId"), "API_TYPE");
    Assertions.assertTrue(rateLimitConfig.isPresent());
    Assertions.assertEquals(rateLimitConfig.get(), new RateLimitConfig(5, 2));
  }
}
