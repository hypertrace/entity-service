package org.hypertrace.entity.query.service.converter.identifier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.quality.Strictness.LENIENT;

import org.hypertrace.entity.query.service.converter.AliasProvider;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = LENIENT)
class IdentifierAliasProviderTest {
  private AliasProvider<ColumnIdentifier> identifierAliasProvider;

  private ColumnIdentifier.Builder columnIdentifierBuilder;

  @BeforeEach
  void setup() {
    columnIdentifierBuilder = ColumnIdentifier.newBuilder().setColumnName("Welcome_Mars");
    identifierAliasProvider = new IdentifierAliasProvider();
  }

  @Test
  void testGetAlias() throws ConversionException {
    assertEquals("Welcome_Mars", identifierAliasProvider.getAlias(columnIdentifierBuilder.build()));
  }

  @Test
  void testGetSetAlias() throws ConversionException {
    ColumnIdentifier columnIdentifier =
        columnIdentifierBuilder.setAlias("Mars_solar_day_in_hours").build();
    assertEquals("Mars_solar_day_in_hours", identifierAliasProvider.getAlias(columnIdentifier));
  }
}
