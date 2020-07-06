package org.hypertrace.entity.service.constants;

public class EntityServiceConstants {

  public static final String TENANT_ID = "tenantId";

  //EntityType constants
  public static final String NAME = "name";
  public static final String VERSION = "version";
  public static final String ATTRIBUTE_TYPE = "attributeType";
  public static final String ID_ATTR = ATTRIBUTE_TYPE + ".identifyingAttribute";

  //Entity constants
  public static final String ID = "_id";
  public static final String ENTITY_TYPE = "entityType";
  public static final String ENTITY_ID = "entityId";
  public static final String ENTITY_NAME = "entityName";
  // this matches with what Docstore automatically inserted
  public static final String ENTITY_CREATED_TIME = "createdTime";

  // Entity Relationship constants
  public static final String ENTITY_RELATIONSHIP_TYPE = "entityRelationshipType";
  public static final String FROM_ENTITY_ID = "fromEntityId";
  public static final String TO_ENTITY_ID = "toEntityId";
}
