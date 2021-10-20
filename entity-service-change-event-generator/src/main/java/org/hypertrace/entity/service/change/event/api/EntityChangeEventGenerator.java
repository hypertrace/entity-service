package org.hypertrace.entity.service.change.event.api;

import java.util.Collection;
import org.hypertrace.core.grpcutils.context.RequestContext;
import org.hypertrace.entity.data.service.v1.Entity;

/** The interface Entity change event generator. */
public interface EntityChangeEventGenerator {

  /**
   * Send create notification for newly added entities
   *
   * @param requestContext requestContext
   * @param entities list of newly created entities
   */
  void sendCreateNotification(RequestContext requestContext, Collection<Entity> entities);

  /**
   * Send delete notification for newly added entities
   *
   * @param requestContext requestContext
   * @param entities list of deleted entities
   */
  void sendDeleteNotification(RequestContext requestContext, Collection<Entity> entities);

  /**
   * Send change notification for created, deleted or updated entities. The entities not present in
   * existing entities collection, however present in updated entities collection are considered to
   * be newly added. The entities present in existing entities collection, however not present in
   * updated entities collection are considered to be deleted. The entities present in both existing
   * and updated entities collection are considered to be updated.
   *
   * @param requestContext requestContext
   * @param existingEntities list of existing entities
   * @param updatedEntities list of updated entities
   */
  void sendChangeNotification(
      RequestContext requestContext,
      Collection<Entity> existingEntities,
      Collection<Entity> updatedEntities);
}
