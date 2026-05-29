// Copyright (c) 2023 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Java SDK,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/sdk-java/blob/main/LICENSE
package dev.restate.sdk.http.vertx;

import dev.restate.common.Slice;
import dev.restate.sdk.core.ExceptionUtils;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import java.util.concurrent.Flow;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

class HttpResponseFlowAdapter implements Flow.Subscriber<Slice> {

  private static final Logger LOG = LogManager.getLogger(HttpResponseFlowAdapter.class);

  private static final long LOG_EVERY_N_WRITES = 64;

  private final HttpServerResponse httpServerResponse;

  private Flow.Subscription outputSubscription;

  private long totalBytesProduced = 0;
  private long writeCount = 0;
  private boolean lastWriteQueueFull = false;
  private long writesWhileQueueFull = 0;
  private long queueFullStartNanos = 0;
  private @Nullable Channel probeChannel;
  private boolean probeAttachAttempted = false;

  HttpResponseFlowAdapter(HttpServerResponse httpServerResponse) {
    this.httpServerResponse = httpServerResponse;

    this.httpServerResponse.exceptionHandler(this::propagateWireFailure);
  }

  @Override
  public void onSubscribe(Flow.Subscription subscription) {
    this.outputSubscription = subscription;
    this.outputSubscription.request(Long.MAX_VALUE);
  }

  @Override
  public void onNext(Slice slice) {
    if (this.httpServerResponse.ended()) {
      cancelSubscription();
      return;
    }

    this.totalBytesProduced += slice.readableBytes();
    this.writeCount++;

    if (!this.probeAttachAttempted) {
      this.probeAttachAttempted = true;
      this.probeChannel = Http2DiagnosticProbes.attachToResponse(this.httpServerResponse);
    }

    boolean queueFull = this.httpServerResponse.writeQueueFull();
    if (queueFull) {
      this.writesWhileQueueFull++;
      if (!this.lastWriteQueueFull) {
        this.queueFullStartNanos = System.nanoTime();
      }
    } else if (this.lastWriteQueueFull) {
      this.queueFullStartNanos = 0;
    }
    if (LOG.isDebugEnabled()
        && (queueFull != this.lastWriteQueueFull || this.writeCount % LOG_EVERY_N_WRITES == 0)) {
      long bytesWritten = this.httpServerResponse.bytesWritten();
      long queueFullDurationMs =
          this.queueFullStartNanos == 0
              ? 0
              : (System.nanoTime() - this.queueFullStartNanos) / 1_000_000L;
      String channelState =
          this.probeChannel == null
              ? ""
              : ", " + Http2DiagnosticProbes.snapshotChannelState(this.probeChannel);
      LOG.debug(
          "Response write: writeQueueFull={}, writeCount={}, totalBytesProduced={}, bytesWritten={}, estimatedPending={}, writesWhileQueueFull={}, queueFullDurationMs={}{}",
          queueFull,
          this.writeCount,
          this.totalBytesProduced,
          bytesWritten,
          this.totalBytesProduced - bytesWritten,
          this.writesWhileQueueFull,
          queueFullDurationMs,
          channelState);
    }
    this.lastWriteQueueFull = queueFull;

    // If HTTP HEADERS frame have not been sent, Vert.x will send them
    this.httpServerResponse.write(
        Buffer.buffer(Unpooled.wrappedBuffer(slice.asReadOnlyByteBuffer())));
  }

  @Override
  public void onError(Throwable throwable) {
    propagatePublisherFailure(throwable);
  }

  @Override
  public void onComplete() {
    endResponse();
  }

  // --- Private operations

  private void propagateWireFailure(Throwable e) {
    LOG.warn("Error from wire", e);
    this.endResponse();
  }

  private void propagatePublisherFailure(Throwable e) {
    if (!httpServerResponse.headWritten()) {
      // Try to write the failure in the head
      ExceptionUtils.findProtocolException(e)
          .ifPresentOrElse(
              pe -> httpServerResponse.setStatusCode(pe.getCode()),
              () -> httpServerResponse.setStatusCode(500));
    }
    LOG.warn("Error from publisher", e);
    this.endResponse();
  }

  private void endResponse() {
    LOG.trace("Closing response");
    if (!this.httpServerResponse.ended()) {
      this.httpServerResponse.end();
    }
    cancelSubscription();
  }

  private void cancelSubscription() {
    if (this.outputSubscription != null) {
      LOG.trace("Cancelling subscription");
      Flow.Subscription outputSubscription = this.outputSubscription;
      this.outputSubscription = null;
      outputSubscription.cancel();
    }
  }
}
