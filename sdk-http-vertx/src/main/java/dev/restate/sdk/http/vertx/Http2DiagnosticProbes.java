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
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Stream;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.Http2ServerConnection;
import java.lang.reflect.Field;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jspecify.annotations.Nullable;

final class Http2DiagnosticProbes {

  private static final Logger LOG = LogManager.getLogger(Http2DiagnosticProbes.class);
  private static final Logger WRITABILITY_LOG =
      LogManager.getLogger("dev.restate.sdk.http.vertx.Http2DiagnosticProbes.writability");
  private static final Logger STREAM_WINDOWS_LOG =
      LogManager.getLogger("dev.restate.sdk.http.vertx.Http2DiagnosticProbes.streamWindows");

  private static final String HTTP2_SERVER_RESPONSE_CLASS =
      "io.vertx.core.http.impl.Http2ServerResponse";
  private static final String WRITABILITY_HANDLER_NAME = "restate-h2-writability-probe";
  private static final long SNAPSHOT_INTERVAL_MS = 200;

  static final boolean ENABLED =
      isTruthy(System.getenv("RESTATE_DIAGNOSTICS_HTTP2"))
          || isTruthy(System.getProperty("restate.diagnostics.http2"));

  private static final AtomicBoolean REFLECTION_BROKEN = new AtomicBoolean(false);
  private static volatile @Nullable Field ctxField;
  private static final Set<Channel> CHANNELS_WITH_HANDLER = ConcurrentHashMap.newKeySet();
  private static final Set<Http2Connection> H2_CONNECTIONS = ConcurrentHashMap.newKeySet();
  private static final AtomicReference<ScheduledExecutorService> SNAPSHOTTER =
      new AtomicReference<>();

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

  static void registerFlowControlSnapshotter(HttpServer server) {
    if (!ENABLED) {
      return;
    }
    server.connectionHandler(
        conn -> {
          if (!(conn instanceof Http2ServerConnection h2ServerConn)) {
            return;
          }
          try {
            Channel channel = h2ServerConn.channel();
            Http2ConnectionHandler handler = channel.pipeline().get(Http2ConnectionHandler.class);
            if (handler == null) {
              return;
            }
            Http2Connection h2 = handler.connection();
            H2_CONNECTIONS.add(h2);
            conn.closeHandler(v -> H2_CONNECTIONS.remove(h2));
            ensureSnapshotterStarted();
          } catch (Throwable t) {
            LOG.debug("Failed to register HTTP/2 connection for window snapshotter", t);
          }
        });
  }

  private static void ensureSnapshotterStarted() {
    if (SNAPSHOTTER.get() != null) {
      return;
    }
    ScheduledExecutorService created =
        Executors.newSingleThreadScheduledExecutor(
            r -> {
              Thread t = new Thread(r, "restate-h2-window-snapshotter");
              t.setDaemon(true);
              return t;
            });
    if (SNAPSHOTTER.compareAndSet(null, created)) {
      created.scheduleAtFixedRate(
          Http2DiagnosticProbes::snapshotTick,
          SNAPSHOT_INTERVAL_MS,
          SNAPSHOT_INTERVAL_MS,
          TimeUnit.MILLISECONDS);
    } else {
      created.shutdownNow();
    }
  }

  private static void snapshotTick() {
    for (Http2Connection h2 : H2_CONNECTIONS) {
      try {
        snapshotConnection(h2);
      } catch (Throwable t) {
        LOG.debug("HTTP/2 window snapshot failed for one connection", t);
      }
    }
  }

  private static void snapshotConnection(Http2Connection h2) throws Exception {
    int[] activeCount = {0};
    h2.forEachActiveStream(
        s -> {
          activeCount[0]++;
          return true;
        });
    if (activeCount[0] == 0) {
      return;
    }
    Http2Stream connStream = h2.connectionStream();
    int connLocal = h2.local().flowController().windowSize(connStream);
    int connRemote = h2.remote().flowController().windowSize(connStream);
    int connHash = System.identityHashCode(h2);
    STREAM_WINDOWS_LOG.warn(
        "h2-windows conn={} active={} connLocalWin={} connRemoteWin={}",
        connHash,
        activeCount[0],
        connLocal,
        connRemote);
    h2.forEachActiveStream(
        s -> {
          STREAM_WINDOWS_LOG.warn(
              "h2-windows conn={} stream={} state={} localWin={} remoteWin={}",
              connHash,
              s.id(),
              s.state(),
              h2.local().flowController().windowSize(s),
              h2.remote().flowController().windowSize(s));
          return true;
        });
  }

  private static boolean isTruthy(String value) {
    return "true".equalsIgnoreCase(value) || "1".equals(value);
  }
}
