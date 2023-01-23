package org.hypertrace.entity.rateLimiter;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import org.hypertrace.core.documentstore.Filter;
import org.hypertrace.core.documentstore.Filter.Op;
import org.hypertrace.core.documentstore.Query;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.attribute.translator.AttributeMetadataIdentifier;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.data.service.v1.AttributeValue;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.data.service.v1.Value;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class EntityRateLimiterTest {

  private static final RequestContext requestContext = RequestContext.forTenantId("tenantId");
  private org.hypertrace.core.documentstore.Collection entitiesCollection;
  private EntityAttributeMapping entityAttributeMapping;
  private EntityRateLimiterConfig entityRateLimiterConfig;
  private Clock clock;

  @BeforeEach
  void setup() throws URISyntaxException {
    this.entitiesCollection = Mockito.mock(org.hypertrace.core.documentstore.Collection.class);
    this.entityAttributeMapping = Mockito.mock(EntityAttributeMapping.class);
    this.entityRateLimiterConfig = new EntityRateLimiterConfig(getRateLimiterConfig());
    this.clock = Mockito.mock(Clock.class);
  }

  @Test
  void testEntityRateLimiter_localLimitReached() {
    long currentTime = System.currentTimeMillis();
    Mockito.when(clock.millis()).thenReturn(currentTime);
    EntityRateLimiter entityRateLimiter =
        new EntityRateLimiter(
            entitiesCollection, entityAttributeMapping, entityRateLimiterConfig, clock);

    Query globalQuery = getGlobalQuery();
    Query windowQuery = getWindowQuery(currentTime);

    Entity entity =
        Entity.newBuilder()
            .setEntityType("API_TYPE")
            .putAttributes(
                "environment",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("envId").build())
                    .build())
            .build();

    Mockito.when(
            entityAttributeMapping.getAttributeMetadataByAttributeId(
                ArgumentMatchers.any(), ArgumentMatchers.eq("API_TYPE.environment")))
        .thenReturn(
            Optional.of(new AttributeMetadataIdentifier("API_TYPE", "attributes.environment")));

    // local limits reached
    Mockito.when(entitiesCollection.total(windowQuery)).thenReturn(3L);
    Mockito.when(entitiesCollection.total(globalQuery)).thenReturn(3L);
    Assertions.assertTrue(
        entityRateLimiter.isRateLimited(requestContext, List.of(), List.of(entity)));
  }

  @Test
  void testEntityRateLimiter_globalLimitReached() {
    long currentTime = System.currentTimeMillis();
    Mockito.when(clock.millis()).thenReturn(currentTime);
    EntityRateLimiter entityRateLimiter =
        new EntityRateLimiter(
            entitiesCollection, entityAttributeMapping, entityRateLimiterConfig, clock);

    Query globalQuery = getGlobalQuery();
    Query windowQuery = getWindowQuery(currentTime);

    Entity entity =
        Entity.newBuilder()
            .setEntityType("API_TYPE")
            .putAttributes(
                "environment",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("envId").build())
                    .build())
            .build();

    Mockito.when(
            entityAttributeMapping.getAttributeMetadataByAttributeId(
                ArgumentMatchers.any(), ArgumentMatchers.eq("API_TYPE.environment")))
        .thenReturn(
            Optional.of(new AttributeMetadataIdentifier("API_TYPE", "attributes.environment")));

    // global limits reached
    Mockito.when(entitiesCollection.total(windowQuery)).thenReturn(1L);
    Mockito.when(entitiesCollection.total(globalQuery)).thenReturn(10L);
    Assertions.assertTrue(
        entityRateLimiter.isRateLimited(requestContext, List.of(), List.of(entity)));
  }

  @Test
  void testEntityRateLimiter_noLimitReached() {
    long currentTime = System.currentTimeMillis();
    Mockito.when(clock.millis()).thenReturn(currentTime);
    EntityRateLimiter entityRateLimiter =
        new EntityRateLimiter(
            entitiesCollection, entityAttributeMapping, entityRateLimiterConfig, clock);

    Query globalQuery = getGlobalQuery();
    Query windowQuery = getWindowQuery(currentTime);

    Entity entity =
        Entity.newBuilder()
            .setEntityType("API_TYPE")
            .putAttributes(
                "environment",
                AttributeValue.newBuilder()
                    .setValue(Value.newBuilder().setString("envId").build())
                    .build())
            .build();

    Mockito.when(
            entityAttributeMapping.getAttributeMetadataByAttributeId(
                ArgumentMatchers.any(), ArgumentMatchers.eq("API_TYPE.environment")))
        .thenReturn(
            Optional.of(new AttributeMetadataIdentifier("API_TYPE", "attributes.environment")));

    // no limits reached
    Mockito.when(entitiesCollection.total(windowQuery)).thenReturn(1L);
    Mockito.when(entitiesCollection.total(globalQuery)).thenReturn(1L);
    Assertions.assertFalse(
        entityRateLimiter.isRateLimited(requestContext, List.of(), List.of(entity)));
  }

  private Query getGlobalQuery() {
    Query globalQuery = new Query();
    Filter[] attrFilters =
        List.of(
                new Filter(Op.EQ, "attribute_id", "attr_val"),
                new Filter(Op.EQ, "attributes.environment.value.string", "envId"))
            .toArray(new Filter[] {});
    Filter attrFilter = new Filter();
    attrFilter.setFieldName("");
    attrFilter.setOp(Op.AND);
    attrFilter.setChildFilters(attrFilters);

    Filter tenantFilter = new Filter(Op.EQ, "tenantId", "tenantId");
    Filter entityTypeFilter = new Filter(Op.EQ, "entityType", "API_TYPE");

    Filter globalQueryFilter = new Filter();
    globalQueryFilter.setOp(Op.AND);
    globalQueryFilter.setChildFilters(
        List.of(tenantFilter, entityTypeFilter, attrFilter).toArray(new Filter[] {}));
    globalQuery.setFilter(globalQueryFilter);
    return globalQuery;
  }

  private Query getWindowQuery(long currentTime) {
    Query windowQuery = new Query();
    Filter[] windowAttrFilters =
        List.of(
                new Filter(Op.GT, "createdTime", currentTime - Duration.ofMinutes(6).toMillis()),
                new Filter(Op.EQ, "attribute_id", "attr_val"),
                new Filter(Op.EQ, "attributes.environment.value.string", "envId"))
            .toArray(new Filter[] {});
    Filter windowAttrFilter = new Filter();
    windowAttrFilter.setFieldName("");
    windowAttrFilter.setOp(Op.AND);
    windowAttrFilter.setChildFilters(windowAttrFilters);

    Filter tenantFilter = new Filter(Op.EQ, "tenantId", "tenantId");
    Filter entityTypeFilter = new Filter(Op.EQ, "entityType", "API_TYPE");

    Filter windowQueryFilter = new Filter();
    windowQueryFilter.setOp(Op.AND);
    windowQueryFilter.setChildFilters(
        List.of(tenantFilter, entityTypeFilter, windowAttrFilter).toArray(new Filter[] {}));
    windowQuery.setFilter(windowQueryFilter);
    return windowQuery;
  }

  private Config getRateLimiterConfig() throws URISyntaxException {
    URI uri =
        getClass().getClassLoader().getResource("rateLimiter/rate_limiter_config.conf").toURI();
    return ConfigFactory.parseFile(new File(uri.getPath()));
  }
}
