package org.hypertrace.entity.attribute.translator;

import static java.util.stream.Collectors.toUnmodifiableMap;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BOOL;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BOOL_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_BYTES;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_DOUBLE;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_DOUBLE_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_INT64;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_INT64_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING_ARRAY;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_STRING_MAP;
import static org.hypertrace.core.attribute.service.v1.AttributeKind.TYPE_TIMESTAMP;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;

public class EntityAttributeMapping {

  private static final String ID_ATTRIBUTE_MAP_CONFIG_PATH = "entity.service.idAttributeMap";
  private static final String ATTRIBUTE_MAP_CONFIG_PATH = "entity.service.attributeMap";
  private static final String ATTRIBUTE_SERVICE_HOST = "attribute.service.config.host";
  private static final String ATTRIBUTE_SERVICE_PORT = "attribute.service.config.port";
  public static final String ENTITY_ATTRIBUTE_DOC_PREFIX = "attributes.";
  public static final String SUB_DOC_PATH = "subDocPath";
  private static final String SCOPE_PATH = "scope";
  private static final String ATTRIBUTE_PATH = "attribute";
  private static final String NAME_PATH = "name";
  private static final Set<AttributeKind> ARRAY_ATTRIBUTE_KINDS =
      Set.of(TYPE_STRING_ARRAY, TYPE_INT64_ARRAY, TYPE_DOUBLE_ARRAY, TYPE_BOOL_ARRAY);
  private static final Set<AttributeKind> PRIMITIVE_ATTRIBUTE_KINDS =
      Set.of(TYPE_DOUBLE, TYPE_INT64, TYPE_BOOL, TYPE_STRING, TYPE_BYTES, TYPE_TIMESTAMP);
  private static final Set<AttributeKind> MAP_ATTRIBUTE_KINDS = Set.of(TYPE_STRING_MAP);

  private final CachingAttributeClient attributeClient;
  private final Map<String, AttributeMetadataIdentifier> explicitAttributeIdByAttributeMetadata;
  private final Map<String, String> idAttributeMap;

  public EntityAttributeMapping(Config config, GrpcChannelRegistry channelRegistry) {
    this(
        CachingAttributeClient.builder(
                channelRegistry.forAddress(
                    config.getString(ATTRIBUTE_SERVICE_HOST),
                    config.getInt(ATTRIBUTE_SERVICE_PORT)))
            .build(),
        config.getConfigList(ATTRIBUTE_MAP_CONFIG_PATH).stream()
            .collect(
                toUnmodifiableMap(
                    conf -> conf.getString(NAME_PATH),
                    conf ->
                        new AttributeMetadataIdentifier(
                            conf.getString(SCOPE_PATH), conf.getString(SUB_DOC_PATH)))),
        config.getConfigList(ID_ATTRIBUTE_MAP_CONFIG_PATH).stream()
            .collect(
                toUnmodifiableMap(
                    conf -> conf.getString(SCOPE_PATH), conf -> conf.getString(ATTRIBUTE_PATH))));
  }

  EntityAttributeMapping(
      CachingAttributeClient attributeClient,
      Map<String, AttributeMetadataIdentifier> attributeIdByAttributeMetadata,
      Map<String, String> idAttributeMap) {
    this.attributeClient = attributeClient;
    this.idAttributeMap = idAttributeMap;
    this.explicitAttributeIdByAttributeMetadata = attributeIdByAttributeMetadata;
  }

  /**
   * Returns the doc store path first looking at the hardcoded service config, then falling back to
   * the attribute name defined in the config service.
   *
   * @return
   */
  public Optional<String> getDocStorePathByAttributeId(
      RequestContext requestContext, String attributeId) {
    Optional<AttributeMetadataIdentifier> attribute =
        Optional.ofNullable(this.explicitAttributeIdByAttributeMetadata.get(attributeId))
            .or(() -> this.calculateAttributeMetadataFromAttributeId(requestContext, attributeId));
    return attribute.map(AttributeMetadataIdentifier::getDocStorePath);
  }

  public Optional<AttributeMetadataIdentifier> getAttributeMetadataByAttributeId(
      RequestContext requestContext, String attributeId) {
    return Optional.ofNullable(this.explicitAttributeIdByAttributeMetadata.get(attributeId))
        .or(() -> this.calculateAttributeMetadataFromAttributeId(requestContext, attributeId));
  }

  public Optional<String> getIdentifierAttributeId(String entityType) {
    return Optional.ofNullable(this.idAttributeMap.get(entityType));
  }

  public Optional<AttributeKind> getAttributeKind(
      final RequestContext requestContext, final String attributeId) {
    return requestContext.call(
        () ->
            this.attributeClient
                .get(attributeId)
                .filter(metadata -> metadata.getSourcesList().contains(AttributeSource.EDS))
                .map(org.hypertrace.core.attribute.service.v1.AttributeMetadata::getValueKind)
                .map(Optional::of)
                .onErrorComplete()
                .defaultIfEmpty(Optional.empty())
                .blockingGet());
  }

  public boolean isArray(final RequestContext requestContext, final String columnId) {
    final Optional<AttributeKind> attributeKind = getAttributeKind(requestContext, columnId);
    return attributeKind.map(this::isArray).orElse(false);
  }

  public boolean isPrimitive(final RequestContext requestContext, final String columnId) {
    final Optional<AttributeKind> attributeKind = getAttributeKind(requestContext, columnId);
    return attributeKind.map(this::isPrimitive).orElse(false);
  }

  public boolean isMap(final RequestContext requestContext, final String columnId) {
    final Optional<AttributeKind> attributeKind = getAttributeKind(requestContext, columnId);
    return attributeKind.map(this::isMap).orElse(false);
  }

  public boolean isArray(final AttributeKind attributeKind) {
    return ARRAY_ATTRIBUTE_KINDS.contains(attributeKind);
  }

  public boolean isPrimitive(final AttributeKind attributeKind) {
    return PRIMITIVE_ATTRIBUTE_KINDS.contains(attributeKind);
  }

  public boolean isMap(final AttributeKind attributeKind) {
    return MAP_ATTRIBUTE_KINDS.contains(attributeKind);
  }

  private Optional<AttributeMetadataIdentifier> calculateAttributeMetadataFromAttributeId(
      RequestContext requestContext, String attributeId) {
    return requestContext.call(
        () ->
            this.attributeClient
                .get(attributeId)
                .filter(metadata -> metadata.getSourcesList().contains(AttributeSource.EDS))
                .map(
                    metadata ->
                        new AttributeMetadataIdentifier(
                            metadata.getScopeString(),
                            ENTITY_ATTRIBUTE_DOC_PREFIX + metadata.getKey()))
                .map(Optional::of)
                .onErrorComplete()
                .defaultIfEmpty(Optional.empty())
                .blockingGet());
  }
}
