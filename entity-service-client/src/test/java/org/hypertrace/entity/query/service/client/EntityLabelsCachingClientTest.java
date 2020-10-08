package org.hypertrace.entity.query.service.client;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.hypertrace.entity.query.service.v1.ColumnIdentifier;
import org.hypertrace.entity.query.service.v1.ColumnMetadata;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.hypertrace.entity.query.service.v1.Expression;
import org.hypertrace.entity.query.service.v1.Filter;
import org.hypertrace.entity.query.service.v1.LiteralConstant;
import org.hypertrace.entity.query.service.v1.Operator;
import org.hypertrace.entity.query.service.v1.ResultSetChunk;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;
import org.hypertrace.entity.query.service.v1.Value;
import org.hypertrace.entity.query.service.v1.ValueType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;

public class EntityLabelsCachingClientTest {
  private static final Map<String, String> TEST_REQUEST_HEADERS = Map.of("k1", "v1",
      "k2", "v2");
  private static final String TENANT_ID = "test-tenant-id";

  @Mock
  private EntityQueryServiceClient entityQueryServiceClient;

  @BeforeEach
  public void setUp() {
    entityQueryServiceClient = mock(EntityQueryServiceClient.class);
  }

  @Test
  public void testGetEntityIdsByLabels() {
    EntityLabelsCachingClient entityLabelsCachingClient = new EntityLabelsCachingClient(entityQueryServiceClient);

    Assertions.assertThrows(UnsupportedOperationException.class, () -> {
      entityLabelsCachingClient.getEntityIdsByLabels(
          "API.id",
          "API.labels",
          List.of("label1", "label2"),
          "API",
          TEST_REQUEST_HEADERS,
          TENANT_ID
      );
    });
  }

  @Test
  public void testGetEntityLabelsForEntities() throws InterruptedException {
    String idColumnName = "API.id";
    String labelsColumnName = "API.labels";
    String type = "API";
    List<String> ids = List.of("api1", "api2", "api3");

    List<String> list1 = List.of("labels10", "labels11");
    List<String> list2 = List.of("labels20");
    List<String> list3 = List.of("labels30", "labels31", "labels32");

    List<List<String>> entityLabelsList = List.of(list1, list2, list3);

    EntityQueryRequest entityQueryRequest = createEntityQueryRequest(idColumnName, labelsColumnName, type, ids);
    Iterator<ResultSetChunk> resultSetChunkIterator = createEntityQueryResponse(
        List.of(idColumnName, labelsColumnName),
        List.of(ValueType.STRING, ValueType.STRING_ARRAY),
        ids,
        entityLabelsList
    );
    when(entityQueryServiceClient.execute(entityQueryRequest, TEST_REQUEST_HEADERS))
        .thenReturn(resultSetChunkIterator);

    EntityLabelsCachingClient entityLabelsCachingClient = new EntityLabelsCachingClient(entityQueryServiceClient);

    Map<String, List<String>> entityLabelsForEntityIds = entityLabelsCachingClient.getEntityLabelsForEntities(
        idColumnName,
        labelsColumnName,
        ids,
        type,
        TEST_REQUEST_HEADERS,
        TENANT_ID);

    Assertions.assertEquals(3, entityLabelsForEntityIds.size());
    Assertions.assertEquals(list1, entityLabelsForEntityIds.get("api1"));
    Assertions.assertEquals(list2, entityLabelsForEntityIds.get("api2"));
    Assertions.assertEquals(list3, entityLabelsForEntityIds.get("api3"));

    // Test refreshing the cache.
    String id = "api2";
    List<String> newLabels = List.of("labels20", "labels21", "labels22");
    EntityQueryRequest entityQueryRequest2 = createEntityQueryRequest(idColumnName, labelsColumnName, type, List.of(id));
    Iterator<ResultSetChunk> resultSetChunkIterator2 = createEntityQueryResponse(
        List.of(idColumnName, labelsColumnName),
        List.of(ValueType.STRING, ValueType.STRING_ARRAY),
        List.of(id),
        List.of(newLabels)
    );
    when(entityQueryServiceClient.execute(entityQueryRequest2, TEST_REQUEST_HEADERS))
        .thenReturn(resultSetChunkIterator2);

    entityLabelsCachingClient.refreshEntityLabelsForEntity(
        idColumnName,
        labelsColumnName,
        id,
        type,
        TEST_REQUEST_HEADERS,
        TENANT_ID);

    // Wait for async call to be made
    Thread.sleep(2000);

    List<String> entityLabelsForEntityId = entityLabelsCachingClient.getEntityLabelsForEntity(
        idColumnName,
        labelsColumnName,
        id,
        type,
        TEST_REQUEST_HEADERS,
        TENANT_ID);

    Assertions.assertEquals(newLabels, entityLabelsForEntityId);
  }

  @Test
  public void testGetEntityLabelsForEntitiesEmptyIds() {
    String idColumnName = "API.id";
    String labelsColumnName = "API.labels";
    String type = "API";
    List<String> ids = List.of();

    List<List<String>> entityLabelsList = List.of();

    EntityQueryRequest entityQueryRequest = createEntityQueryRequest(idColumnName, labelsColumnName, type, ids);
    Iterator<ResultSetChunk> resultSetChunkIterator = createEntityQueryResponse(
        List.of(idColumnName, labelsColumnName),
        List.of(ValueType.STRING, ValueType.STRING_ARRAY),
        ids,
        entityLabelsList
    );
    when(entityQueryServiceClient.execute(entityQueryRequest, TEST_REQUEST_HEADERS))
        .thenReturn(resultSetChunkIterator);

    EntityLabelsCachingClient entityLabelsCachingClient = new EntityLabelsCachingClient(entityQueryServiceClient);

    Map<String, List<String>> entityLabelsForEntityIds = entityLabelsCachingClient.getEntityLabelsForEntities(
        idColumnName,
        labelsColumnName,
        ids,
        type,
        TEST_REQUEST_HEADERS,
        TENANT_ID);

    Assertions.assertEquals(0, entityLabelsForEntityIds.size());
  }

  @Test
  public void testGetEntityLabelsForEntity() {
    String idColumnName = "API.id";
    String labelsColumnName = "API.labels";
    String type = "API";
    String id = "api1";

    List<String> list1 = List.of("labels10", "labels11");
    List<List<String>> entityLabelsList = List.of(list1);

    EntityQueryRequest entityQueryRequest = createEntityQueryRequest(idColumnName, labelsColumnName, type, List.of(id));
    Iterator<ResultSetChunk> resultSetChunkIterator = createEntityQueryResponse(
        List.of(idColumnName, labelsColumnName),
        List.of(ValueType.STRING, ValueType.STRING_ARRAY),
        List.of(id),
        entityLabelsList
    );
    when(entityQueryServiceClient.execute(entityQueryRequest, TEST_REQUEST_HEADERS))
        .thenReturn(resultSetChunkIterator);

    EntityLabelsCachingClient entityLabelsCachingClient = new EntityLabelsCachingClient(entityQueryServiceClient);

    List<String> entityLabelsForEntityId = entityLabelsCachingClient.getEntityLabelsForEntity(
        idColumnName,
        labelsColumnName,
        id,
        type,
        TEST_REQUEST_HEADERS,
        TENANT_ID);

    Assertions.assertEquals(list1, entityLabelsForEntityId);
  }

  private static EntityQueryRequest createEntityQueryRequest(String idColumnName,
                                                      String labelsColumnName,
                                                      String type,
                                                      List<String> ids) {
    return EntityQueryRequest.newBuilder()
        .setEntityType(type)
        .addSelection(createColumnExpression(idColumnName))
        .addSelection(createColumnExpression(labelsColumnName))
        .setFilter(
            Filter.newBuilder()
                .setOperator(Operator.AND)
                .addChildFilter(
                    Filter.newBuilder()
                        .setOperator(Operator.IN)
                        .setLhs(createColumnExpression(idColumnName))
                        .setRhs(createStringArrayLiteralExpression(ids)
                        )
                )
        )
        .setLimit(ids.size())
        .build();
  }

  private static Iterator<ResultSetChunk> createEntityQueryResponse(List<String> columnNames,
                                                                    List<ValueType> valueTypes,
                                                                    List<String> ids,
                                                                    List<List<String>> entityLabelsList) {
    ResultSetChunk.Builder resultSetChunkBuilder = ResultSetChunk.newBuilder();
    ResultSetMetadata.Builder resultSetMetadataBuilder = ResultSetMetadata.newBuilder();
    for (int i = 0; i < columnNames.size(); i++) {
      String columnName = columnNames.get(i);
      ValueType valueType = valueTypes.get(i);
      resultSetMetadataBuilder.addColumnMetadata(
          ColumnMetadata.newBuilder()
              .setColumnName(columnName)
              .setValueType(valueType)
      );
    }

    resultSetChunkBuilder.setResultSetMetadata(resultSetMetadataBuilder);

    for (int i = 0; i < ids.size(); i++) {
      Row.Builder rowBuilder = Row.newBuilder();
      rowBuilder.addColumn(
          Value.newBuilder()
              .setValueType(ValueType.STRING)
              .setString(ids.get(i))
      );
      rowBuilder.addColumn(
          Value.newBuilder()
              .setValueType(ValueType.STRING_ARRAY)
              .addAllStringArray(entityLabelsList.get(i))
      );

      resultSetChunkBuilder.addRow(rowBuilder);
    }

    resultSetChunkBuilder.setIsLastChunk(true);
    return List.of(resultSetChunkBuilder.build()).iterator();
  }

  private static Expression.Builder createColumnExpression(String columnName) {
    return Expression.newBuilder()
        .setColumnIdentifier(ColumnIdentifier.newBuilder().setColumnName(columnName));
  }

  private static Expression.Builder createStringArrayLiteralExpression(List<String> strList) {
    return Expression.newBuilder().setLiteral(
        LiteralConstant.newBuilder().setValue(
            Value.newBuilder()
                .setValueType(ValueType.STRING_ARRAY)
                .addAllStringArray(strList)
        )
    );
  }
}
