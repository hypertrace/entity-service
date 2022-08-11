package org.hypertrace.entity.service.change.event.impl;

import com.typesafe.config.Config;
import java.time.Clock;
import org.hypertrace.entity.query.service.EntityAttributeMapping;
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

  public EntityChangeEventGenerator createEntityChangeEventGenerator(
      Config appConfig, EntityAttributeMapping entityAttributeMapping, Clock clock) {
    if (appConfig.getBoolean(ENTITY_SERVICE_CONFIG_PUBLISH_CHANGE_EVENTS)) {
      return new EntityChangeEventGeneratorImpl(appConfig, entityAttributeMapping, clock);
    } else {
      return new NoopEntityChangeEventGenerator();
    }
  }
}
