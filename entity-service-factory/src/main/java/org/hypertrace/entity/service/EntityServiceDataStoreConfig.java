package org.hypertrace.entity.service;

import com.typesafe.config.Config;

public class EntityServiceDataStoreConfig {

  public static final String DATASTORE_TYPE_CONFIG = "dataStoreType";

  private final String dataStoreType;

  private final Config entityServiceConfig;

  public EntityServiceDataStoreConfig(Config config) {
    entityServiceConfig = config.getConfig("entity.service.config.document.store");
    this.dataStoreType = entityServiceConfig.getString(DATASTORE_TYPE_CONFIG);
  }

  public String getDataStoreType() {
    return dataStoreType;
  }

  public Config getDataStoreConfig() {
    return entityServiceConfig;
  }
}
