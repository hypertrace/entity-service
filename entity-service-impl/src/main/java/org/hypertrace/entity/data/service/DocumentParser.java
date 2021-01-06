package org.hypertrace.entity.data.service;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import java.util.Optional;
import javax.annotation.Nonnull;
import org.hypertrace.core.documentstore.Document;
import org.hypertrace.entity.service.util.DocStoreJsonFormat;
import org.hypertrace.entity.service.util.DocStoreJsonFormat.Parser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DocumentParser {
  private static final Logger LOG = LoggerFactory.getLogger(DocumentParser.class);
  private static final Parser PARSER = DocStoreJsonFormat.parser().ignoringUnknownFields();

  @SuppressWarnings("unchecked")
  <T extends Message> T parseOrThrow(@Nonnull Document document, @Nonnull Message.Builder messageBuilder)
      throws InvalidProtocolBufferException {
    PARSER.merge(document.toJson(), messageBuilder);
    return (T) messageBuilder.build();
  }

  public <T extends Message> Optional<T> parseOrLog(
      @Nonnull Document document, @Nonnull Message.Builder messageBuilder) {
    try {
      return Optional.of(this.parseOrThrow(document, messageBuilder));
    } catch (Throwable throwable) {
      LOG.error(
          "Error processing document into message of type {}: {}",
          messageBuilder.getDescriptorForType().getName(),
          document.toJson(),
          throwable);
      return Optional.empty();
    }
  }
}
