package org.hypertrace.entity.service.change.event.api;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.service.change.event.impl.ChangeResult;

/** The interface Entity change event generator. */
public interface EntityChangeEventGenerator {

  /**
   * Send notification for entities
   *
   * @param requestContext requestContext
   * @param changeResult contains all created, deleted and update entities
   */
  void sendChangeNotification(RequestContext requestContext, ChangeResult changeResult);
}
