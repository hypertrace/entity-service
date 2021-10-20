package org.hypertrace.entity.service.change.event.impl;

import java.util.Collection;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;

/** No-op implementation of Entity change event generator interface. */
public class NoopEntityChangeEventGenerator implements EntityChangeEventGenerator {

  NoopEntityChangeEventGenerator() {}

  @Override
  public void sendCreateNotification(
      RequestContext requestContext, Collection<Entity> entities, long eventTimeMillis) {
    // No-op
  }

  @Override
  public void sendDeleteNotification(
      RequestContext requestContext, Collection<Entity> entities, long eventTimeMillis) {
    // No-op
  }

  @Override
  public void sendChangeNotification(
      RequestContext requestContext,
      Collection<Entity> existingEntities,
      Collection<Entity> updatedEntities,
      long eventTimeMillis) {
    // No-op
  }
}
