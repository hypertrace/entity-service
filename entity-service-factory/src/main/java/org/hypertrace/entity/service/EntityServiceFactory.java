package org.hypertrace.entity.service;

import static java.util.Objects.isNull;

import com.typesafe.config.Config;
import io.grpc.Channel;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import java.time.Clock;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformService;
import org.hypertrace.core.serviceframework.grpc.GrpcPlatformServiceFactory;
import org.hypertrace.core.serviceframework.grpc.GrpcServiceContainerEnvironment;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.data.service.EntityDataServiceImpl;
import org.hypertrace.entity.metric.EntityCounterMetricSender;
import org.hypertrace.entity.query.service.EntityQueryServiceImpl;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;
import org.hypertrace.entity.service.change.event.impl.EntityChangeEventGeneratorFactory;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceImpl;

public class EntityServiceFactory implements GrpcPlatformServiceFactory {

  private static final String SERVICE_NAME = "entity-service";
  private static final String DATA_STORE_REPORTING_NAME = "entity-service-datastore";

  private Datastore datastore;
  private GrpcServiceContainerEnvironment grpcServiceContainerEnvironment;

  @Override
  public List<GrpcPlatformService> buildServices(
      GrpcServiceContainerEnvironment grpcServiceContainerEnvironment) {
    this.grpcServiceContainerEnvironment = grpcServiceContainerEnvironment;
    Config config = grpcServiceContainerEnvironment.getConfig(SERVICE_NAME);
    EntityServiceDataStoreConfig dataStoreConfig = new EntityServiceDataStoreConfig(config);
    this.datastore =
        DatastoreProvider.getDatastore(
            dataStoreConfig.getDataStoreType(), dataStoreConfig.getDataStoreConfig());
    EntityAttributeMapping entityAttributeMapping =
        new EntityAttributeMapping(config, grpcServiceContainerEnvironment.getChannelRegistry());
    EntityChangeEventGenerator entityChangeEventGenerator =
        EntityChangeEventGeneratorFactory.getInstance()
            .createEntityChangeEventGenerator(config, entityAttributeMapping, Clock.systemUTC());
    Channel localChannel =
        grpcServiceContainerEnvironment
            .getChannelRegistry()
            .forName(grpcServiceContainerEnvironment.getInProcessChannelName());
    EntityCounterMetricSender entityCounterMetricSender = new EntityCounterMetricSender();
    return Stream.of(
            new org.hypertrace.entity.type.service.EntityTypeServiceImpl(datastore),
            new EntityTypeServiceImpl(datastore),
            new EntityDataServiceImpl(
                datastore, localChannel, entityChangeEventGenerator, entityCounterMetricSender),
            new EntityQueryServiceImpl(
                datastore,
                config,
                entityAttributeMapping,
                entityChangeEventGenerator,
                entityCounterMetricSender,
                localChannel))
        .map(GrpcPlatformService::new)
        .collect(Collectors.toUnmodifiableList());
  }

  public void checkAndReportDataStoreHealth() {
    if (isNull(this.datastore) || isNull(this.grpcServiceContainerEnvironment)) {
      return;
    }

    if (this.datastore.healthCheck()) {
      this.grpcServiceContainerEnvironment.reportServiceStatus(
          DATA_STORE_REPORTING_NAME, ServingStatus.SERVING);
    } else {
      this.grpcServiceContainerEnvironment.reportServiceStatus(
          DATA_STORE_REPORTING_NAME, ServingStatus.NOT_SERVING);
    }
  }
}
