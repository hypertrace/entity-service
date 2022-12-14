package org.hypertrace.entity.service.change.event.impl;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.service.change.event.api.EntityChangeEventGenerator;

/** No-op implementation of Entity change event generator interface. */
public class NoopEntityChangeEventGenerator implements EntityChangeEventGenerator {

  NoopEntityChangeEventGenerator() {}

  @Override
  public void sendChangeNotification(RequestContext requestContext, ChangeResult changeResult) {
    // No-op
  }
}
