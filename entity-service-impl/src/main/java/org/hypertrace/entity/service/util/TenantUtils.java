package org.hypertrace.entity.service.util;

import java.util.List;

public class TenantUtils {

  public static final String ROOT_TENANT_ID = "__root";

  /**
   * For now the hierarchy is hardcoded to add just the __root tenant
   */
  public static List<String> getTenantHierarchy(String tenantId) {
    return List.of(ROOT_TENANT_ID, tenantId);
  }
}
