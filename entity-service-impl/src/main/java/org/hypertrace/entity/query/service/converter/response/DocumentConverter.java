package org.hypertrace.entity.query.service.converter.response;

import org.hypertrace.core.documentstore.Document;
import org.hypertrace.entity.query.service.converter.ConversionException;
import org.hypertrace.entity.query.service.v1.ResultSetMetadata;
import org.hypertrace.entity.query.service.v1.Row;

public interface DocumentConverter {
  Row convertToRow(final Document document, final ResultSetMetadata resultSetMetadata)
      throws ConversionException;
}
