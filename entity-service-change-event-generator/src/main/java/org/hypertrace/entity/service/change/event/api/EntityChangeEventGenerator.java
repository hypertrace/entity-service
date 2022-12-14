package org.hypertrace.entity.service.change.event.api;

import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.service.change.event.impl.ChangeResult;

/** The interface Entity change event generator. */
public interface EntityChangeEventGenerator {

  void sendChangeNotification(RequestContext requestContext, ChangeResult changeResult);
}
