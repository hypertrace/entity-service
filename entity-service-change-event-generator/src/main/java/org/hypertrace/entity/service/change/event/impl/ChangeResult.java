package org.hypertrace.entity.service.change.event.impl;

import java.util.Collection;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.hypertrace.entity.data.service.v1.Entity;

@AllArgsConstructor
@Getter
public class ChangeResult {
  private Collection<Entity> createdEntity;
  private Map<Entity, Entity> existingToUpdatedEntitiesMap;
  private Collection<Entity> deletedEntity;
}
