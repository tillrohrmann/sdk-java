// Copyright (c) 2023 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Java SDK,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/sdk-java/blob/main/LICENSE
package dev.restate.sdk.http.vertx;

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

final class Http2DiagnosticProbes {

  private static final Logger LOG = LogManager.getLogger(Http2DiagnosticProbes.class);
  private static final Logger WRITABILITY_LOG =
      LogManager.getLogger("dev.restate.sdk.http.vertx.Http2DiagnosticProbes.writability");

  private static final String HTTP2_SERVER_RESPONSE_CLASS =
      "io.vertx.core.http.impl.Http2ServerResponse";
  private static final String WRITABILITY_HANDLER_NAME = "restate-h2-writability-probe";

  static final boolean ENABLED =
      isTruthy(System.getenv("RESTATE_DIAGNOSTICS_HTTP2"))
          || isTruthy(System.getProperty("restate.diagnostics.http2"));

  private static final AtomicBoolean REFLECTION_BROKEN = new AtomicBoolean(false);
  private static volatile @Nullable Field ctxField;
  private static final Set<Channel> CHANNELS_WITH_HANDLER = ConcurrentHashMap.newKeySet();

  private Http2DiagnosticProbes() {}

  static HttpServerOptions configureOptions(HttpServerOptions options) {
    if (!ENABLED) {
      return options;
    }
    return new HttpServerOptions(options).setLogActivity(true);
  }

  static @Nullable Channel attachToResponse(HttpServerResponse response) {
    if (!ENABLED || REFLECTION_BROKEN.get()) {
      return null;
    }
    if (!HTTP2_SERVER_RESPONSE_CLASS.equals(response.getClass().getName())) {
      return null;
    }
    try {
      Field field = ctxField;
      if (field == null) {
        field = response.getClass().getDeclaredField("ctx");
        field.setAccessible(true);
        ctxField = field;
      }
      ChannelHandlerContext ctx = (ChannelHandlerContext) field.get(response);
      if (ctx == null) {
        return null;
      }
      Channel channel = ctx.channel();
      installWritabilityHandler(channel);
      return channel;
    } catch (Throwable t) {
      if (REFLECTION_BROKEN.compareAndSet(false, true)) {
        LOG.warn(
            "HTTP/2 diagnostic probes disabled: reflective access to Http2ServerResponse failed",
            t);
      }
      return null;
    }
  }

  private static void installWritabilityHandler(Channel channel) {
    if (!CHANNELS_WITH_HANDLER.add(channel)) {
      return;
    }
    try {
      if (channel.pipeline().get(WRITABILITY_HANDLER_NAME) != null) {
        return;
      }
      channel
          .pipeline()
          .addFirst(
              WRITABILITY_HANDLER_NAME,
              new ChannelInboundHandlerAdapter() {
                @Override
                public void channelWritabilityChanged(ChannelHandlerContext ctx) throws Exception {
                  Channel ch = ctx.channel();
                  WRITABILITY_LOG.warn(
                      "channelWritabilityChanged: isWritable={}, beforeUnwritable={}, beforeWritable={}",
                      ch.isWritable(),
                      ch.bytesBeforeUnwritable(),
                      ch.bytesBeforeWritable());
                  super.channelWritabilityChanged(ctx);
                }

                @Override
                public void channelInactive(ChannelHandlerContext ctx) throws Exception {
                  CHANNELS_WITH_HANDLER.remove(ctx.channel());
                  super.channelInactive(ctx);
                }
              });
    } catch (Throwable t) {
      CHANNELS_WITH_HANDLER.remove(channel);
      LOG.debug("Failed to install writability handler", t);
    }
  }

  static String snapshotChannelState(Channel channel) {
    long pending = -1L;
    try {
      var outbound = channel.unsafe().outboundBuffer();
      if (outbound != null) {
        pending = outbound.totalPendingWriteBytes();
      }
    } catch (Throwable t) {
      // outbound buffer may be null during close; leave pending=-1
    }
    return String.format(
        "isWritable=%s, beforeUnwritable=%d, beforeWritable=%d, autoRead=%s, pendingWriteBytes=%d",
        channel.isWritable(),
        channel.bytesBeforeUnwritable(),
        channel.bytesBeforeWritable(),
        channel.config().isAutoRead(),
        pending);
  }

  private static boolean isTruthy(String value) {
    return "true".equalsIgnoreCase(value) || "1".equals(value);
  }
}
