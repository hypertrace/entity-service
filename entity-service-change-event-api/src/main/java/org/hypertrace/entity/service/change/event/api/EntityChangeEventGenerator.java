package org.hypertrace.entity.service.change.event.api;

import java.util.Map;
import org.hypertrace.entity.data.service.v1.Entity;

/** The interface Entity change event generator. */
public interface EntityChangeEventGenerator {

  /**
   * Send change notification for any change to the entities
   *
   * @param existingEntityMap the map of existing entities
   * @param updatedEntityMap the map of updated entities
   */
  void sendChangeNotification(
      Map<String, Entity> existingEntityMap, Map<String, Entity> updatedEntityMap);
}
