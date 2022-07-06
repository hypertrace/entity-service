package org.hypertrace.entity.service;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;

public class EntityService extends StandAloneGrpcPlatformServiceContainer {

  private final EntityServiceFactory entityServiceFactory = new EntityServiceFactory();
  private ScheduledFuture<?> dataStoreReportingFuture;

  @Override
  protected GrpcPlatformServiceFactory getServiceFactory() {
    return entityServiceFactory;
  }

  @Override
  protected void doStart() {
    this.dataStoreReportingFuture = this.startReportingDataStoreHealth();
    super.doStart();
  }

  @Override
  protected void doStop() {
    this.dataStoreReportingFuture.cancel(false);
    super.doStop();
  }

  public EntityService(ConfigClient configClient) {
    super(configClient);
  }

  private ScheduledFuture<?> startReportingDataStoreHealth() {
    return Executors.newSingleThreadScheduledExecutor()
        .scheduleAtFixedRate(
            entityServiceFactory::checkAndReportDataStoreHealth, 10, 60, TimeUnit.SECONDS);
  }
}
