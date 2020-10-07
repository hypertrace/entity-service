package org.hypertrace.entity.query.service.client;

import java.util.List;
import java.util.Map;

public interface EntityLabelsClient {
  List<String> getEntityLabelsForEntity(String idColumnName,
                                        String labelsColumnName,
                                        String id,
                                        String type,
                                        Map<String, String> headers,
                                        String tenantId);

  Map<String, List<String>> getEntityLabelsForEntities(String idColumnName,
                                                       String labelsColumnName,
                                                       List<String> ids,
                                                       String type,
                                                       Map<String, String> headers,
                                                       String tenantId);

  List<String> getEntitiesWithLabels(String idColumnName,
                                     String labelsColumnName,
                                     List<String> labels,
                                     String type,
                                     Map<String, String> headers,
                                     String tenantId);
}
