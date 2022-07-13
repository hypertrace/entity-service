package org.hypertrace.entity.service;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;

import java.util.List;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServerDefinition;
import org.hypertrace.core.serviceframework.grpc.PlatformPeriodicTaskDefinition;
import org.hypertrace.core.serviceframework.grpc.StandAloneGrpcPlatformServiceContainer;

public class EntityService extends StandAloneGrpcPlatformServiceContainer {

  private final EntityServiceFactory entityServiceFactory = new EntityServiceFactory();

  public EntityService(ConfigClient configClient) {
    super(configClient);
    this.registerManagedPeriodicTask(
        PlatformPeriodicTaskDefinition.builder()
            .name("Data Store health check")
            .runnable(this.entityServiceFactory::checkAndReportDataStoreHealth)
            .initialDelay(ofSeconds(10))
            .period(ofMinutes(1))
            .build());
  }

  @Override
  protected List<GrpcPlatformServerDefinition> getServerDefinitions() {
    return List.of(
        GrpcPlatformServerDefinition.builder()
            .name(this.getServiceName())
            .port(this.getServicePort())
            .serviceFactory(this.entityServiceFactory)
            .build());
  }
}
