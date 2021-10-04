package org.hypertrace.entity.service.change.event.impl;

import java.util.Collection;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;

/** No-op implementation of Entity change event generator interface. */
public class NoopEntityChangeEventGenerator implements EntityChangeEventGenerator {

  NoopEntityChangeEventGenerator() {}

  @Override
  public void sendCreateNotification(Collection<Entity> entities) {
    // No-op
  }

  @Override
  public void sendDeleteNotification(Collection<Entity> entities) {
    // No-op
  }

  @Override
  public void sendChangeNotification(
      Collection<Entity> existingEntities, Collection<Entity> updatedEntities) {
    // No-op
  }
}
