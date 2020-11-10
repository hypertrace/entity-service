package org.hypertrace.entity.service;

import com.typesafe.config.Config;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.hypertrace.core.documentstore.Datastore;
import org.hypertrace.core.documentstore.DatastoreProvider;
import org.hypertrace.core.grpcutils.server.InterceptorUtil;
import org.hypertrace.core.serviceframework.PlatformService;
import org.hypertrace.core.serviceframework.config.ConfigClient;
import org.hypertrace.entity.data.service.EntityDataServiceImpl;
import org.hypertrace.entity.query.service.EntityQueryServiceImpl;
import org.hypertrace.entity.type.service.v2.EntityTypeServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityService extends PlatformService {

  private static final Logger LOG = LoggerFactory.getLogger(EntityService.class);
  private static final String SERVICE_NAME_CONFIG = "service.name";
  private static final String SERVICE_PORT_CONFIG = "service.port";
  private static final String ENTITY_SERVICE_CONFIG = "entity.service.config";

  private String serviceName;
  private Datastore datastore;
  private Server server;

  private final ScheduledExecutorService scheduledExecutorService =
      Executors.newSingleThreadScheduledExecutor();
  private int consecutiveFailedHealthCheck = 0;

  public EntityService(ConfigClient configClient) {
    super(configClient);
  }

  @Override
  protected void doInit() {
    serviceName = getAppConfig().getString(SERVICE_NAME_CONFIG);
    int port = getAppConfig().getInt(SERVICE_PORT_CONFIG);
    Config config = getAppConfig().getConfig(ENTITY_SERVICE_CONFIG);
    EntityServiceConfig entityServiceConfig = new EntityServiceConfig(config);
    Config dataStoreConfig =
        entityServiceConfig.getDataStoreConfig(entityServiceConfig.getDataStoreType());
    this.datastore =
        DatastoreProvider.getDatastore(entityServiceConfig.getDataStoreType(), dataStoreConfig);
    ManagedChannel localChannel =
        ManagedChannelBuilder.forAddress("localhost", port).usePlaintext().build();
    this.getLifecycle().shutdownComplete().thenRun(localChannel::shutdown);
    server = ServerBuilder.forPort(port)
        .addService(InterceptorUtil.wrapInterceptors(new org.hypertrace.entity.type.service.EntityTypeServiceImpl(datastore)))
        .addService(InterceptorUtil.wrapInterceptors(new EntityTypeServiceImpl(datastore)))
        .addService(InterceptorUtil.wrapInterceptors(new EntityDataServiceImpl(datastore, localChannel)))
        .addService(
            InterceptorUtil.wrapInterceptors(new EntityQueryServiceImpl(datastore, getAppConfig())))
        .build();
    scheduledExecutorService.scheduleAtFixedRate(() -> {
      if (!datastore.healthCheck()) {
        consecutiveFailedHealthCheck++;
        if (consecutiveFailedHealthCheck > 5) {
          LOG.warn("Failed 5 times to connect to data store, shut down EntityService...");
          System.exit(2);
        }
      } else {
        consecutiveFailedHealthCheck = 0;
      }
    }, 60, 60, TimeUnit.SECONDS);
  }

  @Override
  protected void doStart() {
    LOG.info("Starting Entity Data Service");
    try {
      server.start();
      server.awaitTermination();
    } catch (IOException e) {
      LOG.error("Fail to start the server.");
      throw new RuntimeException(e);
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(ie);
    }
  }

  @Override
  protected void doStop() {
    LOG.info("Shutting down service: {}", serviceName);
    while (!server.isShutdown()) {
      server.shutdown();
      try {
        Thread.sleep(100);
      } catch (InterruptedException ignore) {
      }
    }
  }

  @Override
  public boolean healthCheck() {
    return true;
  }

  @Override
  public String getServiceName() {
    return serviceName;
  }
}
