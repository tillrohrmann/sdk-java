package dev.restate.sdk.vertx.testservices;

import dev.restate.sdk.blocking.RestateBlockingService;
import dev.restate.sdk.blocking.RestateContext;
import dev.restate.sdk.core.StateKey;
import dev.restate.sdk.core.TypeTag;
import dev.restate.sdk.core.impl.testservices.GreeterGrpc;
import dev.restate.sdk.core.impl.testservices.GreetingRequest;
import dev.restate.sdk.core.impl.testservices.GreetingResponse;
import io.grpc.stub.StreamObserver;
import java.nio.charset.StandardCharsets;

public class BlockingGreeterService extends GreeterGrpc.GreeterImplBase
    implements RestateBlockingService {

  public static final StateKey<Long> COUNTER =
      StateKey.of(
          "counter",
          TypeTag.using(
              l -> l.toString().getBytes(StandardCharsets.UTF_8),
              v -> Long.parseLong(new String(v, StandardCharsets.UTF_8))));

  @Override
  public void greet(GreetingRequest request, StreamObserver<GreetingResponse> responseObserver) {
    RestateContext ctx = restateContext();

    var count = ctx.get(COUNTER).orElse(0L) + 1;
    ctx.set(COUNTER, count);

    responseObserver.onNext(
        GreetingResponse.newBuilder()
            .setMessage("Hello " + request.getName() + ". Count: " + count)
            .build());
    responseObserver.onCompleted();
  }
}