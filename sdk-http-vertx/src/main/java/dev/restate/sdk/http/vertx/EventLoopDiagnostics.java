// Copyright (c) 2023 - Restate Software, Inc., Restate GmbH
//
// This file is part of the Restate Java SDK,
// which is released under the MIT license.
//
// You can find a copy of the license in file LICENSE in the root
// directory of this repository or package, or at
// https://github.com/restatedev/sdk-java/blob/main/LICENSE
package dev.restate.sdk.http.vertx;

import io.netty.channel.EventLoopGroup;
import io.netty.util.concurrent.SingleThreadEventExecutor;
import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

final class EventLoopDiagnostics {

  private static final Logger HEARTBEAT_LOG =
      LogManager.getLogger("dev.restate.sdk.http.vertx.EventLoopDiagnostics.heartbeat");
  private static final Logger QUEUE_DEPTH_LOG =
      LogManager.getLogger("dev.restate.sdk.http.vertx.EventLoopDiagnostics.queueDepth");
  private static final Logger LOG = LogManager.getLogger(EventLoopDiagnostics.class);

  private static final long HEARTBEAT_INTERVAL_MS = 50;
  private static final long QUEUE_DEPTH_INTERVAL_MS = 200;
  private static final long SCHEDULING_DELAY_THRESHOLD_MS = 25;
  private static final int PENDING_TASKS_THRESHOLD = 50;

  private static final Set<Vertx> ATTACHED = ConcurrentHashMap.newKeySet();
  private static final AtomicReference<ScheduledExecutorService> SCHEDULER =
      new AtomicReference<>();

  private EventLoopDiagnostics() {}

  static void attachIfEnabled(Vertx vertx) {
    if (!isEnabled()) {
      return;
    }
    if (!ATTACHED.add(vertx)) {
      return;
    }
    ScheduledExecutorService scheduler = getOrCreateScheduler();
    Context probeContext = vertx.getOrCreateContext();

    scheduler.scheduleAtFixedRate(
        () -> heartbeatTick(probeContext),
        HEARTBEAT_INTERVAL_MS,
        HEARTBEAT_INTERVAL_MS,
        TimeUnit.MILLISECONDS);

    scheduler.scheduleAtFixedRate(
        () -> queueDepthTick(vertx),
        QUEUE_DEPTH_INTERVAL_MS,
        QUEUE_DEPTH_INTERVAL_MS,
        TimeUnit.MILLISECONDS);

    LOG.info("Event-loop diagnostics attached to Vertx instance");
  }

  private static boolean isEnabled() {
    return isTruthy(System.getenv("RESTATE_DIAGNOSTICS_EVENT_LOOP"))
        || isTruthy(System.getProperty("restate.diagnostics.eventLoop"));
  }

  private static boolean isTruthy(String value) {
    return "true".equalsIgnoreCase(value) || "1".equals(value);
  }

  private static ScheduledExecutorService getOrCreateScheduler() {
    ScheduledExecutorService existing = SCHEDULER.get();
    if (existing != null) {
      return existing;
    }
    ScheduledExecutorService created =
        Executors.newScheduledThreadPool(
            2,
            r -> {
              Thread t = new Thread(r);
              t.setDaemon(true);
              t.setName("restate-eventloop-probe-" + t.getId());
              return t;
            });
    if (SCHEDULER.compareAndSet(null, created)) {
      return created;
    }
    created.shutdownNow();
    return SCHEDULER.get();
  }

  private static void heartbeatTick(Context probeContext) {
    try {
      long t0 = System.nanoTime();
      probeContext.runOnContext(
          v -> {
            long delayMs = (System.nanoTime() - t0) / 1_000_000L;
            if (delayMs > SCHEDULING_DELAY_THRESHOLD_MS) {
              HEARTBEAT_LOG.warn("eventloop scheduling delay: {} ms", delayMs);
            }
          });
    } catch (Throwable t) {
      LOG.debug("Heartbeat probe failed", t);
    }
  }

  private static void queueDepthTick(Vertx vertx) {
    try {
      EventLoopGroup group = ((VertxInternal) vertx).getEventLoopGroup();
      int idx = 0;
      for (var exec : group) {
        if (exec instanceof SingleThreadEventExecutor) {
          int pending = ((SingleThreadEventExecutor) exec).pendingTasks();
          if (pending > PENDING_TASKS_THRESHOLD) {
            QUEUE_DEPTH_LOG.warn("ev-loop[{}] pendingTasks={}", idx, pending);
          }
        }
        idx++;
      }
    } catch (Throwable t) {
      LOG.debug("Queue depth probe failed", t);
    }
  }
}
