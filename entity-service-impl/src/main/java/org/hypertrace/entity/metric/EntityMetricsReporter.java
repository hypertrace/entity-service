package org.hypertrace.entity.metric;

import static org.hypertrace.core.documentstore.expression.operators.AggregationOperator.COUNT;
import static org.hypertrace.core.documentstore.model.config.CustomMetricConfig.VALUE_KEY;

import java.time.Duration;
import java.util.List;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.expression.impl.AggregateExpression;
import org.hypertrace.core.documentstore.expression.impl.ConstantExpression;
import org.hypertrace.core.documentstore.expression.impl.IdentifierExpression;
import org.hypertrace.core.documentstore.expression.impl.LogicalExpression;
import org.hypertrace.core.documentstore.expression.impl.RelationalExpression;
import org.hypertrace.core.documentstore.expression.operators.RelationalOperator;
import org.hypertrace.core.documentstore.model.config.CustomMetricConfig;
import org.hypertrace.core.documentstore.query.Filter;
import org.hypertrace.core.documentstore.query.Query;
import org.hypertrace.core.serviceframework.docstore.metrics.DocStoreCustomMetricReportingConfig;
import org.hypertrace.core.serviceframework.docstore.metrics.DocStoreMetricsRegistry;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;
import org.hypertrace.entity.v1.entitytype.EntityType;

public class EntityMetricsReporter {

  private static final String ALL_API_COUNT_METRIC_NAME = "all.api.entities.count";
  private static final String RAW_ENTITIES_COLLECTION = "raw_entities";
  private static final String API_DISCOVERY_STATE_ENTITY_PATH =
      "attributes.api_discovery_state.value.string";
  private static final String TENANT_ID_ENTITY_PATH = "tenantId";
  private static final String ENTITY_TYPE_ENTITY_PATH = "entityType";
  private static final String DISCOVERED = "DISCOVERED";
  private static final String UNDER_DISCOVERY = "UNDER_DISCOVERY";
  private final DocStoreMetricsRegistry metricsRegistry;

  public EntityMetricsReporter(
      final Datastore datastore, final PlatformServiceLifecycle lifecycle) {
    metricsRegistry = new DocStoreMetricsRegistry(datastore).withPlatformLifecycle(lifecycle);
  }

  public void monitor() {
    metricsRegistry.monitor();
  }

  @SuppressWarnings("FieldCanBeLocal")
  private final List<DocStoreCustomMetricReportingConfig> apiCounterConfig =
      List.of(
          DocStoreCustomMetricReportingConfig.builder()
              .reportingInterval(Duration.ofHours(1))
              .config(
                  CustomMetricConfig.builder()
                      .metricName(ALL_API_COUNT_METRIC_NAME)
                      .collectionName(RAW_ENTITIES_COLLECTION)
                      .query(
                          Query.builder()
                              .setFilter(getFilter())
                              .addSelection(IdentifierExpression.of(TENANT_ID_ENTITY_PATH))
                              .addSelection(
                                  IdentifierExpression.of(ENTITY_TYPE_ENTITY_PATH), "entity_type")
                              .addSelection(
                                  AggregateExpression.of(COUNT, ConstantExpression.of(1)),
                                  VALUE_KEY)
                              .addAggregation(IdentifierExpression.of(TENANT_ID_ENTITY_PATH))
                              .addAggregation(IdentifierExpression.of(ENTITY_TYPE_ENTITY_PATH))
                              .build())
                      .build())
              .build());

  private Filter getFilter() {
    return Filter.builder()
        .expression(
            LogicalExpression.and(
                List.of(
                    RelationalExpression.of(
                        IdentifierExpression.of(ENTITY_TYPE_ENTITY_PATH),
                        RelationalOperator.EQ,
                        ConstantExpression.of(EntityType.API.name())),
                    RelationalExpression.of(
                        IdentifierExpression.of(API_DISCOVERY_STATE_ENTITY_PATH),
                        RelationalOperator.IN,
                        ConstantExpression.ofStrings(List.of(UNDER_DISCOVERY, DISCOVERED))))))
        .build();
  }
}
