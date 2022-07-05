package org.hypertrace.entity.service;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;

public class EntityService extends StandAloneGrpcPlatformServiceContainer {

  private final EntityServiceFactory entityServiceFactory = new EntityServiceFactory();

  @Override
  protected GrpcPlatformServiceFactory getServiceFactory() {
    return entityServiceFactory;
  }

  @Override
  protected void doStart() {
    super.doStart();
    this.startReportingDataStoreHealth();
  }

  public EntityService(ConfigClient configClient) {
    super(configClient);
  }

  private void startReportingDataStoreHealth() {
    Executors.newSingleThreadScheduledExecutor()
        .scheduleAtFixedRate(
            entityServiceFactory::checkAndReportDataStoreHealth, 10, 60, TimeUnit.SECONDS);
  }
}
