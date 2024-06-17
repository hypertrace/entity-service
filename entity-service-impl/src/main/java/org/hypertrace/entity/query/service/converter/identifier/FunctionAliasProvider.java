package org.hypertrace.entity.query.service.converter.identifier;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import lombok.AllArgsConstructor;
import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.v1.Function;
import org.hypertrace.entity.service.util.StringUtils;

@Singleton
@AllArgsConstructor(onConstructor_ = {@Inject})
public class FunctionAliasProvider implements AliasProvider<Function> {

  @Override
  public String getAlias(final Function function) {
    if (StringUtils.isNotBlank(function.getAlias())) {
      return function.getAlias();
    }

    throw new IllegalArgumentException("Function alias is missing");
  }
}
