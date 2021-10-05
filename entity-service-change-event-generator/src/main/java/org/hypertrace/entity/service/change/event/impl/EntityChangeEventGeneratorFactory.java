package org.hypertrace.entity.service.change.event.impl;

import com.typesafe.config.Config;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;

public class EntityChangeEventGeneratorFactory {

  static final String ENTITY_SERVICE_CONFIG_PUBLISH_CHANGE_EVENTS =
      "entity.service.config.publish.change.events";

  private static final EntityChangeEventGeneratorFactory instance =
      new EntityChangeEventGeneratorFactory();

  private EntityChangeEventGeneratorFactory() {}

  public static EntityChangeEventGeneratorFactory getInstance() {
    return instance;
  }

  public EntityChangeEventGenerator createEntityChangeEventGenerator(Config appConfig) {
    if (appConfig.getBoolean(ENTITY_SERVICE_CONFIG_PUBLISH_CHANGE_EVENTS)) {
      return new EntityChangeEventGeneratorImpl(appConfig);
    } else {
      return new NoopEntityChangeEventGenerator();
    }
  }
}
