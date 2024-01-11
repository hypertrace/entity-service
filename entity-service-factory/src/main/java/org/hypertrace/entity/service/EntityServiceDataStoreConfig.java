package org.hypertrace.entity.service;

import com.typesafe.config.Config;
import org.hypertrace.core.documentstore.model.config.DatastoreConfig;
import org.hypertrace.core.documentstore.model.config.TypesafeConfigDatastoreConfigExtractor;

public class EntityServiceDataStoreConfig {

  private static final String DATASTORE_TYPE_CONFIG = "dataStoreType";

  private final Config documentStoreConfig;

  public EntityServiceDataStoreConfig(Config config) {
    documentStoreConfig = config.getConfig("entity.service.config.document.store");
  }

  public DatastoreConfig getDataStoreConfig() {
    return TypesafeConfigDatastoreConfigExtractor.from(documentStoreConfig, DATASTORE_TYPE_CONFIG)
        .extract();
  }
}
