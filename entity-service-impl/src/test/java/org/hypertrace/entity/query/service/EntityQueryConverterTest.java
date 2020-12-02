package org.hypertrace.entity.query.service;


import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Collections;
import org.hypertrace.entity.data.service.v1.Query;
import org.hypertrace.entity.query.service.v1.EntityQueryRequest;
import org.junit.jupiter.api.Test;

public class EntityQueryConverterTest {

  @Test
  public void test_convertToEDSQuery_limitAndOffset() {
    // no offset and limit specified
    EntityQueryRequest request = EntityQueryRequest.newBuilder().build();
    Query convertedQuery = EntityQueryConverter.convertToEDSQuery(request, Collections.emptyMap());
    assertEquals(0, convertedQuery.getOffset());
    assertEquals(0, convertedQuery.getLimit());

    int limit = 3;
    int offset = 1;
    request = EntityQueryRequest.newBuilder().setLimit(limit).setOffset(offset).build();
    convertedQuery = EntityQueryConverter.convertToEDSQuery(request, Collections.emptyMap());
    assertEquals(limit, convertedQuery.getLimit());
    assertEquals(offset, convertedQuery.getOffset());
  }

}
