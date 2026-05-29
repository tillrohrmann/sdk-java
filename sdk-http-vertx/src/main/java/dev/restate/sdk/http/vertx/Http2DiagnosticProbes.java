// Copyright (c) 2023 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Java SDK,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/sdk-java/blob/main/LICENSE
package dev.restate.sdk.http.vertx;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.codec.http2.Http2Connection;
import io.netty.handler.codec.http2.Http2ConnectionHandler;
import io.netty.handler.codec.http2.Http2Stream;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.Http2ServerConnection;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLongArray;
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
  private static final Logger OUTBOUND_FRAMES_LOG =
      LogManager.getLogger("dev.restate.sdk.http.vertx.Http2DiagnosticProbes.outboundFrames");

  private static final String HTTP2_SERVER_RESPONSE_CLASS =
      "io.vertx.core.http.impl.Http2ServerResponse";
  private static final String WRITABILITY_HANDLER_NAME = "restate-h2-writability-probe";
  private static final String OUTBOUND_FRAME_HANDLER_NAME = "restate-h2-outbound-frame-probe";
  private static final long SNAPSHOT_INTERVAL_MS = 200;
  private static final int FRAME_TYPE_SLOTS = 10;

  static final boolean ENABLED =
      isTruthy(System.getenv("RESTATE_DIAGNOSTICS_HTTP2"))
          || isTruthy(System.getProperty("restate.diagnostics.http2"));

  private static final AtomicBoolean REFLECTION_BROKEN = new AtomicBoolean(false);
  private static volatile @Nullable Field ctxField;
  private static final Set<Channel> CHANNELS_WITH_HANDLER = ConcurrentHashMap.newKeySet();
  private static final Set<Http2Connection> H2_CONNECTIONS = ConcurrentHashMap.newKeySet();
  private static final Map<Channel, OutboundFrameStats> FRAME_STATS = new ConcurrentHashMap<>();
  private static final AtomicReference<ScheduledExecutorService> SNAPSHOTTER =
      new AtomicReference<>();

  private static final class OutboundFrameStats {
    final AtomicLongArray writtenCount = new AtomicLongArray(FRAME_TYPE_SLOTS);
    final AtomicLongArray inflightCount = new AtomicLongArray(FRAME_TYPE_SLOTS);
    final AtomicLongArray bytesWritten = new AtomicLongArray(FRAME_TYPE_SLOTS);
    final AtomicLongArray maxLatencyNs = new AtomicLongArray(FRAME_TYPE_SLOTS);
  }

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
            FRAME_STATS.computeIfAbsent(channel, c -> new OutboundFrameStats());
            installOutboundFrameHandler(channel);
            conn.closeHandler(
                v -> {
                  H2_CONNECTIONS.remove(h2);
                  FRAME_STATS.remove(channel);
                });
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
    for (Map.Entry<Channel, OutboundFrameStats> e : FRAME_STATS.entrySet()) {
      try {
        snapshotFrameStats(e.getKey(), e.getValue());
      } catch (Throwable t) {
        LOG.debug("Outbound frame snapshot failed for one channel", t);
      }
    }
  }

  private static void snapshotFrameStats(Channel channel, OutboundFrameStats stats) {
    int connHash = System.identityHashCode(channel);
    for (int type = 0; type < FRAME_TYPE_SLOTS; type++) {
      long written = stats.writtenCount.getAndSet(type, 0);
      if (written == 0) {
        continue;
      }
      long bytes = stats.bytesWritten.getAndSet(type, 0);
      long maxLatNs = stats.maxLatencyNs.getAndSet(type, 0);
      long inflight = stats.inflightCount.get(type);
      OUTBOUND_FRAMES_LOG.warn(
          "outbound-frame conn={} type={} writtenInTick={} inflight={} bytes={} maxLatencyMs={}",
          connHash,
          frameTypeName(type),
          written,
          inflight,
          bytes,
          maxLatNs / 1_000_000L);
    }
  }

  private static void installOutboundFrameHandler(Channel channel) {
    try {
      ChannelPipeline pipeline = channel.pipeline();
      if (pipeline.get(OUTBOUND_FRAME_HANDLER_NAME) != null) {
        return;
      }
      String http2HandlerName = null;
      for (Map.Entry<String, ChannelHandler> e : pipeline.toMap().entrySet()) {
        if (e.getValue() instanceof Http2ConnectionHandler) {
          http2HandlerName = e.getKey();
          break;
        }
      }
      OutboundFrameProbe probe = new OutboundFrameProbe();
      if (http2HandlerName != null) {
        pipeline.addBefore(http2HandlerName, OUTBOUND_FRAME_HANDLER_NAME, probe);
      } else {
        pipeline.addFirst(OUTBOUND_FRAME_HANDLER_NAME, probe);
      }
    } catch (Throwable t) {
      LOG.debug("Failed to install outbound frame probe", t);
    }
  }

  private static final class OutboundFrameProbe extends ChannelDuplexHandler {
    @Override
    public void handlerAdded(ChannelHandlerContext ctx) throws Exception {
      LOG.info("OutboundFrameProbe installed on conn={}", System.identityHashCode(ctx.channel()));
      super.handlerAdded(ctx);
    }

    @Override
    public void write(ChannelHandlerContext ctx, Object msg, ChannelPromise promise)
        throws Exception {
      if (msg instanceof ByteBuf buf) {
        OutboundFrameStats stats = FRAME_STATS.get(ctx.channel());
        if (stats != null) {
          int[] types = walkFrameTypes(buf, stats);
          if (types.length > 0) {
            long t0 = System.nanoTime();
            promise.addListener(
                f -> {
                  long dt = System.nanoTime() - t0;
                  for (int type : types) {
                    stats.inflightCount.decrementAndGet(type);
                    long prev;
                    do {
                      prev = stats.maxLatencyNs.get(type);
                      if (dt <= prev) break;
                    } while (!stats.maxLatencyNs.compareAndSet(type, prev, dt));
                  }
                });
          }
        }
      }
      super.write(ctx, msg, promise);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
      FRAME_STATS.remove(ctx.channel());
      super.channelInactive(ctx);
    }
  }

  private static int[] walkFrameTypes(ByteBuf buf, OutboundFrameStats stats) {
    int idx = buf.readerIndex();
    int end = buf.writerIndex();
    int count = 0;
    int[] tmp = new int[8];
    while (idx + 9 <= end) {
      int length = buf.getUnsignedMedium(idx);
      int type = buf.getByte(idx + 3) & 0xff;
      int frameEnd = idx + 9 + length;
      if (frameEnd > end) {
        break;
      }
      if (type < FRAME_TYPE_SLOTS) {
        stats.writtenCount.incrementAndGet(type);
        stats.inflightCount.incrementAndGet(type);
        stats.bytesWritten.addAndGet(type, 9L + length);
        if (count == tmp.length) {
          int[] grown = new int[tmp.length * 2];
          System.arraycopy(tmp, 0, grown, 0, count);
          tmp = grown;
        }
        tmp[count++] = type;
      }
      idx = frameEnd;
    }
    if (count == tmp.length) {
      return tmp;
    }
    int[] out = new int[count];
    System.arraycopy(tmp, 0, out, 0, count);
    return out;
  }

  private static String frameTypeName(int type) {
    return switch (type) {
      case 0 -> "DATA";
      case 1 -> "HEADERS";
      case 2 -> "PRIORITY";
      case 3 -> "RST_STREAM";
      case 4 -> "SETTINGS";
      case 5 -> "PUSH_PROMISE";
      case 6 -> "PING";
      case 7 -> "GOAWAY";
      case 8 -> "WINDOW_UPDATE";
      case 9 -> "CONTINUATION";
      default -> "TYPE_" + type;
    };
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
