package org.hypertrace.entity.service.exception;

/**
 * Exception to be thrown when the incoming request to the GRPC service doesn't have all required
 * fields populated.
 */
public class InvalidRequestException extends Exception {

  public InvalidRequestException(String msg) {
    super(msg);
  }
}
