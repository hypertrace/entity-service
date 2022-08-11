package org.hypertrace.entity.common;

import static java.util.stream.Collectors.toUnmodifiableMap;

import com.typesafe.config.Config;
import java.util.Map;
import java.util.Optional;
import org.hypertrace.core.attribute.service.cachingclient.CachingAttributeClient;
import org.hypertrace.core.attribute.service.v1.AttributeKind;
import org.hypertrace.core.attribute.service.v1.AttributeMetadata;
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

  private final CachingAttributeClient attributeClient;
  private final Map<String, String> explicitDocStoreMappingsByAttributeId;
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
                    conf -> conf.getString("name"), conf -> conf.getString(SUB_DOC_PATH))),
        config.getConfigList(ID_ATTRIBUTE_MAP_CONFIG_PATH).stream()
            .collect(
                toUnmodifiableMap(
                    conf -> conf.getString("scope"), conf -> conf.getString("attribute"))));
  }

  EntityAttributeMapping(
      CachingAttributeClient attributeClient,
      Map<String, String> explicitDocStoreMappingsByAttributeId,
      Map<String, String> idAttributeMap) {
    this.attributeClient = attributeClient;
    this.explicitDocStoreMappingsByAttributeId = explicitDocStoreMappingsByAttributeId;
    this.idAttributeMap = idAttributeMap;
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

  public Optional<String> getDocStoreAttributeNameByAttributeId(
      RequestContext requestContext, String attributeId) {
    Optional<String> docStorePath = this.getDocStorePathByAttributeId(requestContext, attributeId);
    return docStorePath.map(s -> removePrefix(s, ENTITY_ATTRIBUTE_DOC_PREFIX));
  }

  public Optional<String> getIdentifierAttributeId(String entityType) {
    return Optional.ofNullable(this.idAttributeMap.get(entityType));
  }

  public boolean isMultiValued(RequestContext requestContext, String attributeId) {
    return requestContext.call(
        () ->
            this.attributeClient
                .get(attributeId)
                .filter(metadata -> metadata.getSourcesList().contains(AttributeSource.EDS))
                .map(AttributeMetadata::getValueKind)
                .map(valueKind -> (AttributeKind.TYPE_STRING_ARRAY == valueKind))
                .onErrorComplete()
                .defaultIfEmpty(false)
                .blockingGet());
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

  private String removePrefix(String str, final String prefix) {
    if (str != null && prefix != null && str.startsWith(prefix)) {
      return str.substring(prefix.length());
    }
    return str;
  }
}
