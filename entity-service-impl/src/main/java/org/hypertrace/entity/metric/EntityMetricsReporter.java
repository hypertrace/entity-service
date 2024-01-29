package org.hypertrace.entity.metric;

import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.serviceframework.docstore.metrics.DocStoreMetricsRegistry;
import org.hypertrace.core.serviceframework.spi.PlatformServiceLifecycle;

public class EntityMetricsReporter {
  private final DocStoreMetricsRegistry metricsRegistry;

  public EntityMetricsReporter(
      final Datastore datastore, final PlatformServiceLifecycle lifecycle) {
    metricsRegistry = new DocStoreMetricsRegistry(datastore).withPlatformLifecycle(lifecycle);
  }

  public void monitor() {
    metricsRegistry.monitor();
  }
}
