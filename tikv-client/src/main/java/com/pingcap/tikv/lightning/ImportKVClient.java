package com.pingcap.tikv.lightning;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.AbstractGRPCClient;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.operation.NoopHandler;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.ConcreteBackOffer;
import io.grpc.ManagedChannel;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.tikv.kvproto.ImportKVGrpc;
import org.tikv.kvproto.ImportKVGrpc.ImportKVBlockingStub;
import org.tikv.kvproto.ImportKVGrpc.ImportKVStub;
import org.tikv.kvproto.ImportKvpb.CleanupEngineRequest;
import org.tikv.kvproto.ImportKvpb.CleanupEngineResponse;
import org.tikv.kvproto.ImportKvpb.CloseEngineRequest;
import org.tikv.kvproto.ImportKvpb.CloseEngineResponse;
import org.tikv.kvproto.ImportKvpb.ImportEngineRequest;
import org.tikv.kvproto.ImportKvpb.ImportEngineResponse;
import org.tikv.kvproto.ImportKvpb.KVPair;
import org.tikv.kvproto.ImportKvpb.OpenEngineRequest;
import org.tikv.kvproto.ImportKvpb.OpenEngineResponse;
import org.tikv.kvproto.ImportKvpb.WriteEngineResponse;
import org.tikv.kvproto.ImportKvpb.WriteEngineV3Request;

public class ImportKVClient extends AbstractGRPCClient<ImportKVBlockingStub, ImportKVStub> {
  public OpenEngineResponse openEngine(ByteString uuid) {
    createChannel();
    Supplier<OpenEngineRequest> request =
        () -> OpenEngineRequest.newBuilder().setUuid(uuid).build();

    NoopHandler<OpenEngineResponse> noopHandler = new NoopHandler<>();

    return callWithRetry(
        ConcreteBackOffer.newCustomBackOff(1),
        ImportKVGrpc.METHOD_OPEN_ENGINE,
        request,
        noopHandler);
  }

  private Random random = new Random();

  private void createChannel() {
    ManagedChannel channel =
        channelFactory.getChannel(importAddrs.get(random.nextInt(importAddrs.size())));
    this.blockingStub = ImportKVGrpc.newBlockingStub(channel);
    this.asyncStub = ImportKVGrpc.newStub(channel);
  }

  public void closeEngine(ByteString uuid) {
    createChannel();
    Supplier<CloseEngineRequest> request =
        () -> CloseEngineRequest.newBuilder().setUuid(uuid).build();
    NoopHandler<CloseEngineResponse> noopHandler = new NoopHandler<>();

    callWithRetry(
        ConcreteBackOffer.newCustomBackOff(1),
        ImportKVGrpc.METHOD_CLOSE_ENGINE,
        request,
        noopHandler);
  }

  public void cleanupEngine(ByteString uuid) {
    createChannel();
    Supplier<CleanupEngineRequest> request =
        () -> CleanupEngineRequest.newBuilder().setUuid(uuid).build();

    NoopHandler<CleanupEngineResponse> noopHandler = new NoopHandler<>();

    callWithRetry(
        ConcreteBackOffer.newCustomBackOff(1),
        ImportKVGrpc.METHOD_CLEANUP_ENGINE,
        request,
        noopHandler);
  }

  public void writeRowsV3(
      ByteString uuid, String tblName, String[] colsNames, long ts, List<KVPair> kvs) {
    createChannel();
    Supplier<WriteEngineV3Request> request =
        () -> {
          WriteEngineV3Request.Builder builder =
              WriteEngineV3Request.newBuilder().setCommitTs(ts).setUuid(uuid);
          for (int i = 0; i < kvs.size(); i++) {
            builder.setPairs(i, kvs.get(i));
          }

          return builder.build();
        };

    // TODO check error
    NoopHandler<WriteEngineResponse> noopHandler = new NoopHandler<>();
    callWithRetry(
        ConcreteBackOffer.newCustomBackOff(1),
        ImportKVGrpc.METHOD_WRITE_ENGINE_V3,
        request,
        noopHandler);
  }

  public void importEngine(ByteString uuid) {
    createChannel();
    Supplier<ImportEngineRequest> request =
        () -> ImportEngineRequest.newBuilder().setPdAddr(this.pdAddr).setUuid(uuid).build();

    NoopHandler<ImportEngineResponse> noopHandler = new NoopHandler<>();
    callWithRetry(
        ConcreteBackOffer.newCustomBackOff(1),
        ImportKVGrpc.METHOD_IMPORT_ENGINE,
        request,
        noopHandler);
  }

  // TODO: revisit it later.
  public void compactCluster(
      int level, int dbId, int numOfThreads, ByteString start, ByteString end) {}

  protected ImportKVClient(TiConfiguration conf, ChannelFactory channelFactory) {
    super(conf, channelFactory);
    List<String> pdAddrs =
        conf.getPdAddrs()
            .stream()
            .map(x -> String.format("%s:%s", x.getHost(), x.getPort()))
            .collect(Collectors.toList());
    if (pdAddrs.isEmpty()) {
      throw new IllegalArgumentException("pd addrs is empty");
    }
    importAddrs =
        conf.getImporterAddrs()
            .stream()
            .map(x -> String.format("%s:%s", x.getHost(), x.getPort()))
            .collect(Collectors.toList());
    pdAddr = pdAddrs.get(0);
  }

  private String pdAddr;
  private List<String> importAddrs;

  @Override
  protected ImportKVBlockingStub getBlockingStub() {
    return blockingStub;
  }

  @Override
  protected ImportKVStub getAsyncStub() {
    return asyncStub;
  }

  @Override
  public void close() throws Exception {
    if (channelFactory != null) {
      channelFactory.close();
    }
  }
}
