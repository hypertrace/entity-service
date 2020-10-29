package org.hypertrace.entity.type.service.rxclient;

import java.util.Objects;
import javax.annotation.Nonnull;
import org.hypertrace.core.grpcutils.client.rx.GrpcRxExecutionContext;
import org.hypertrace.core.grpcutils.context.RequestContext;

class TenantBasedCacheKey {
  static TenantBasedCacheKey forCurrentContext() {
    return new TenantBasedCacheKey(Objects.requireNonNull(RequestContext.CURRENT.get()));
  }

  private static final String DEFAULT_IDENTITY = "default";

  private final GrpcRxExecutionContext executionContext;
  private final String tenantId;

  protected TenantBasedCacheKey(@Nonnull RequestContext requestContext) {
    this.executionContext = GrpcRxExecutionContext.forContext(requestContext);
    this.tenantId = requestContext.getTenantId().orElse(DEFAULT_IDENTITY);
  }

  public GrpcRxExecutionContext getExecutionContext() {
    return executionContext;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    TenantBasedCacheKey that = (TenantBasedCacheKey) o;
    return tenantId.equals(that.tenantId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenantId);
  }
}
