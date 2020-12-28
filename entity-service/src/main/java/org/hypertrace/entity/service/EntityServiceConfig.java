package org.hypertrace.entity.service;

import com.typesafe.config.Config;
import org.hypertrace.core.documentstore.DocumentStoreConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntityServiceConfig {

  private static final Logger LOG = LoggerFactory.getLogger(EntityServiceConfig.class);

  private final String dataStoreType;

  private final Config entityServiceConfig;

  public EntityServiceConfig(Config config) {
    entityServiceConfig = config.getConfig("entity-service");
    dataStoreType = entityServiceConfig.getString(DocumentStoreConfig.DATASTORE_TYPE_CONFIG_KEY);
  }

  public String getDataStoreType() {
    return dataStoreType;
  }

  public Config getDataStoreConfig(String storeType) {
    entityServiceConfig.entrySet().forEach(e -> LOG.info("Config key {}", e.getKey()));
    return entityServiceConfig.getConfig(storeType);
  }
}
