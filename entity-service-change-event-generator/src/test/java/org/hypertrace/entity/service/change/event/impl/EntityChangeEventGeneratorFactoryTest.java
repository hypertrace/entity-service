package org.hypertrace.entity.service.change.event.impl;

import static org.hypertrace.entity.service.change.event.impl.EntityChangeEventGeneratorFactory.ENTITY_SERVICE_CONFIG_PUBLISH_CHANGE_EVENTS;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.time.Clock;
import java.util.List;
import java.util.Map;
import org.hypertrace.entity.attribute.translator.EntityAttributeMapping;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

class EntityChangeEventGeneratorFactoryTest {

  @Test
  void createNoopEntityChangeEventGenerator() {
    EntityAttributeMapping entityAttributeMapping = Mockito.mock(EntityAttributeMapping.class);
    Config config =
        ConfigFactory.parseMap(Map.of(ENTITY_SERVICE_CONFIG_PUBLISH_CHANGE_EVENTS, "false"));
    EntityChangeEventGenerator entityChangeEventGenerator =
        EntityChangeEventGeneratorFactory.getInstance()
            .createEntityChangeEventGenerator(config, entityAttributeMapping, Clock.systemUTC());
    assertTrue(entityChangeEventGenerator instanceof NoopEntityChangeEventGenerator);
  }

  @Test
  void createEntityChangeEventGeneratorImpl() {
    EntityAttributeMapping entityAttributeMapping = Mockito.mock(EntityAttributeMapping.class);
    Config config = getEventStoreConfig();
    EntityChangeEventGenerator entityChangeEventGenerator =
        EntityChangeEventGeneratorFactory.getInstance()
            .createEntityChangeEventGenerator(config, entityAttributeMapping, Clock.systemUTC());
    assertTrue(entityChangeEventGenerator instanceof EntityChangeEventGeneratorImpl);
  }

  private Config getEventStoreConfig() {
    return ConfigFactory.parseMap(
        Map.of(
            "entity.service.change.skip.attributes",
            List.of(),
            ENTITY_SERVICE_CONFIG_PUBLISH_CHANGE_EVENTS,
            "true",
            "event.store",
            Map.of(
                "type",
                "kafka",
                "bootstrap.servers",
                "localhost:9092",
                "entity.change.events.producer",
                Map.of(
                    "topic.name",
                    "entity-change-events",
                    "bootstrap.servers",
                    "localhost:9092",
                    "key.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer",
                    "value.serializer",
                    "org.apache.kafka.common.serialization.StringSerializer",
                    "schema.registry.url",
                    "http://localhost:8081"))));
  }
}
