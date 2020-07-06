package org.hypertrace.entity.service.util;

import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TenantUtilsTest {

  @Test
  public void testGetTenantHierarchy() {
    Assertions.assertEquals("__root", TenantUtils.ROOT_TENANT_ID);
    Assertions.assertEquals(List.of(TenantUtils.ROOT_TENANT_ID, "test-tenant-id"),
        TenantUtils.getTenantHierarchy("test-tenant-id"));
  }
}
