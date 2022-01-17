package org.hypertrace.entity.query.service.converter;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.service.util.StringUtils;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class IdentifierAliasProvider implements AliasProvider<ColumnIdentifier> {

  @Override
  public String getAlias(final ColumnIdentifier identifier) {
    if (StringUtils.isNotBlank(identifier.getAlias())) {
      return identifier.getAlias();
    }

    return identifier.getColumnName();
  }
}
