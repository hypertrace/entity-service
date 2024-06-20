package org.hypertrace.entity.service.change.event.impl;

import static java.util.function.Function.identity;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import org.hypertrace.entity.data.service.v1.Entity;

public class EntityChangeEvaluator {

  public static ChangeResult evaluateChange(
      Collection<Entity> existingEntities, Collection<Entity> updatedEntities) {
    Map<String, Entity> existingEntityMap =
        existingEntities.stream()
            .collect(Collectors.toMap(Entity::getEntityId, identity(), (v1, v2) -> v2));
    Map<String, Entity> upsertedEntityMap =
        updatedEntities.stream()
            .collect(Collectors.toMap(Entity::getEntityId, identity(), (v1, v2) -> v2));
    MapDifference<String, Entity> mapDifference =
        Maps.difference(existingEntityMap, upsertedEntityMap);

    java.util.Collection<Entity> createdEntities = mapDifference.entriesOnlyOnRight().values();

    Map<Entity, Entity> existingToUpdatedEntitiesMap =
        mapDifference.entriesDiffering().entrySet().stream()
            .collect(
                Collectors.toMap(
                    entry -> entry.getValue().leftValue(), entry -> entry.getValue().rightValue()));

    java.util.Collection<Entity> deletedEntities = mapDifference.entriesOnlyOnLeft().values();

    return new ChangeResult(createdEntities, existingToUpdatedEntitiesMap, deletedEntities);
  }
}
