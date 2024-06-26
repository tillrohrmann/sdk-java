// Copyright (c) 2023 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Java SDK,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/sdk-java/blob/main/LICENSE
package dev.restate.sdk.common;

import com.google.protobuf.ByteString;
import io.opentelemetry.context.Context;
import java.util.Map;
import java.util.Objects;

public final class Request {

  private final InvocationId invocationId;
  private final Context otelContext;
  private final ByteString body;
  private final Map<String, String> headers;

  public Request(
      InvocationId invocationId,
      Context otelContext,
      ByteString body,
      Map<String, String> headers) {
    this.invocationId = invocationId;
    this.otelContext = otelContext;
    this.body = body;
    this.headers = headers;
  }

  public InvocationId invocationId() {
    return invocationId;
  }

  public Context otelContext() {
    return otelContext;
  }

  public byte[] body() {
    return body.toByteArray();
  }

  public ByteString bodyBuffer() {
    return body;
  }

  public Map<String, String> headers() {
    return headers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Request request = (Request) o;
    return Objects.equals(invocationId, request.invocationId)
        && Objects.equals(otelContext, request.otelContext)
        && Objects.equals(body, request.body)
        && Objects.equals(headers, request.headers);
  }

  @Override
  public int hashCode() {
    int result = Objects.hashCode(invocationId);
    result = 31 * result + Objects.hashCode(otelContext);
    result = 31 * result + Objects.hashCode(body);
    result = 31 * result + Objects.hashCode(headers);
    return result;
  }

  @Override
  public String toString() {
    return "Request{" + "invocationId=" + invocationId + ", headers=" + headers + '}';
  }
}
