package org.hypertrace.entity.query.service;

import static java.util.stream.Collectors.toUnmodifiableMap;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
import org.hypertrace.core.attribute.service.v1.AttributeSource;
import org.hypertrace.core.grpcutils.client.GrpcChannelRegistry;
import org.hypertrace.core.grpcutils.context.RequestContext;

class EntityAttributeMapping {
  private static final String ATTRIBUTE_MAP_CONFIG_PATH = "entity.service.attributeMap";
  private static final String ATTRIBUTE_SERVICE_HOST = "attribute.service.config.host";
  private static final String ATTRIBUTE_SERVICE_PORT = "attribute.service.config.port";
  static final String ENTITY_ATTRIBUTE_DOC_PREFIX = "attributes.";
  public static final String TYPE_STRING_ARRAY = "TYPE_STRING_ARRAY";
  public static final String VALUE_KIND = "valueKind";
  public static final String SUB_DOC_PATH = "subDocPath";

  private final CachingAttributeClient attributeClient;
  private final Map<String, String> explicitDocStoreMappingsByAttributeId;
  private final Map<String, Boolean> multiValuedAttributeMap;

  EntityAttributeMapping(Config config, GrpcChannelRegistry channelRegistry) {
    this(
        CachingAttributeClient.builder(
                channelRegistry.forAddress(
                    config.getString(ATTRIBUTE_SERVICE_HOST),
                    config.getInt(ATTRIBUTE_SERVICE_PORT)))
            .build(),
        config.getConfigList(ATTRIBUTE_MAP_CONFIG_PATH).stream()
            .collect(
                toUnmodifiableMap(
                    conf -> conf.getString("name"), conf -> conf.getString(SUB_DOC_PATH))),
        config.getConfigList(ATTRIBUTE_MAP_CONFIG_PATH).stream()
            .filter(conf -> conf.hasPath(VALUE_KIND))
            .collect(
                toUnmodifiableMap(
                    conf -> conf.getString("name"),
                    conf -> TYPE_STRING_ARRAY.equals(conf.getString(VALUE_KIND)))));
  }

  EntityAttributeMapping(
      CachingAttributeClient attributeClient,
      Map<String, String> explicitDocStoreMappingsByAttributeId,
      Map<String, Boolean> multiValuedAttributeMap) {
    this.attributeClient = attributeClient;
    this.explicitDocStoreMappingsByAttributeId = explicitDocStoreMappingsByAttributeId;
    this.multiValuedAttributeMap = multiValuedAttributeMap;
  }

  /**
   * Returns the doc store path first looking at the hardcoded service config, then falling back to
   * the attribute name defined in the config service.
   *
   * @return
   */
  public Optional<String> getDocStorePathByAttributeId(
      RequestContext requestContext, String attributeId) {
    return Optional.ofNullable(this.explicitDocStoreMappingsByAttributeId.get(attributeId))
        .or(() -> this.calculateDocStorePathFromAttributeId(requestContext, attributeId));
  }

  public boolean isMultiValued(RequestContext requestContext, String attributeId) {
    if (multiValuedAttributeMap.containsKey(attributeId)) {
      return multiValuedAttributeMap.get(attributeId);
    }
    return getValueKindFromAttributeId(requestContext, attributeId);
  }

  private Optional<String> calculateDocStorePathFromAttributeId(
      RequestContext requestContext, String attributeId) {
    return requestContext.call(
        () ->
            this.attributeClient
                .get(attributeId)
                .filter(metadata -> metadata.getSourcesList().contains(AttributeSource.EDS))
                .map(AttributeMetadata::getKey)
                .map(key -> ENTITY_ATTRIBUTE_DOC_PREFIX + key)
                .map(Optional::of)
                .onErrorComplete()
                .defaultIfEmpty(Optional.empty())
                .blockingGet());
  }

  private boolean getValueKindFromAttributeId(RequestContext requestContext, String attributeId) {
    return requestContext.call(
        () ->
            this.attributeClient
                .get(attributeId)
                .filter(metadata -> metadata.getSourcesList().contains(AttributeSource.EDS))
                .map(AttributeMetadata::getValueKind)
                .map(key -> TYPE_STRING_ARRAY.equals(key.name()))
                .onErrorComplete()
                .defaultIfEmpty(false)
                .blockingGet());
  }
}