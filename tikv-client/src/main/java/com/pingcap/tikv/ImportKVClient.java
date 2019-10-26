package com.pingcap.tikv;

import com.google.protobuf.ByteString;
import com.pingcap.tikv.operation.NoopHandler;
import com.pingcap.tikv.util.ChannelFactory;
import com.pingcap.tikv.util.ConcreteBackOffer;
import java.util.function.Supplier;
import org.tikv.kvproto.ImportKVGrpc;
import org.tikv.kvproto.ImportKVGrpc.ImportKVBlockingStub;
import org.tikv.kvproto.ImportKVGrpc.ImportKVStub;
import org.tikv.kvproto.ImportKvpb.CleanupEngineRequest;
import org.tikv.kvproto.ImportKvpb.CleanupEngineResponse;
import org.tikv.kvproto.ImportKvpb.CloseEngineRequest;
import org.tikv.kvproto.ImportKvpb.CloseEngineResponse;
import org.tikv.kvproto.ImportKvpb.ImportEngineRequest;
import org.tikv.kvproto.ImportKvpb.ImportEngineResponse;
import org.tikv.kvproto.ImportKvpb.OpenEngineRequest;
import org.tikv.kvproto.ImportKvpb.OpenEngineResponse;
import org.tikv.kvproto.ImportKvpb.WriteEngineRequest;
import org.tikv.kvproto.ImportKvpb.WriteEngineResponse;
import org.tikv.kvproto.ImportKvpb.WriteEngineV3Request;

public class ImportKVClient extends AbstractGRPCClient<ImportKVBlockingStub, ImportKVStub> {

  public void openEngine(ByteString keyPrefix, ByteString uuid) {
    Supplier<OpenEngineRequest> request =
        () -> OpenEngineRequest.newBuilder().setKeyPrefix(keyPrefix).setUuid(uuid).build();

    NoopHandler<OpenEngineResponse> noopHandler = new NoopHandler<>();

    callWithRetry(
        ConcreteBackOffer.newCustomBackOff(1),
        ImportKVGrpc.METHOD_OPEN_ENGINE,
        request,
        noopHandler);
  }

  public void closeEngine(ByteString uuid) {
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
    Supplier<CleanupEngineRequest> request =
        () -> CleanupEngineRequest.newBuilder().setUuid(uuid).build();

    NoopHandler<CleanupEngineResponse> noopHandler = new NoopHandler<>();

    callWithRetry(
        ConcreteBackOffer.newCustomBackOff(1),
        ImportKVGrpc.METHOD_CLEANUP_ENGINE,
        request,
        noopHandler);
  }

  public WriteEngineResponse writeEngine() {
    // TODO revist it later
    //		WriteBatch batch = WriteBatch.newBuilder().setCommitTs(0).setMutations(0, ).build();
    Supplier<WriteEngineRequest> request =
        () ->
            WriteEngineRequest.newBuilder()
                //			.setBatch()
                //			.setHead()
                .build();

    NoopHandler<WriteEngineResponse> noopHandler = new NoopHandler<>();
    return callWithRetry(
        ConcreteBackOffer.newCustomBackOff(1),
        ImportKVGrpc.METHOD_WRITE_ENGINE,
        request,
        noopHandler);
  }

  public void writeEngineV3() {
    //		WriteBatch batch = WriteBatch.newBuilder().setCommitTs(0).setMutations(0, ).build();
    Supplier<WriteEngineV3Request> request =
        () ->
            WriteEngineV3Request.newBuilder()
                //			.setBatch()
                //			.setHead()
                .build();

    NoopHandler<WriteEngineResponse> noopHandler = new NoopHandler<>();
    callWithRetry(
        ConcreteBackOffer.newCustomBackOff(1),
        ImportKVGrpc.METHOD_WRITE_ENGINE_V3,
        request,
        noopHandler);
  }

  public void importEngine(String pdAddr, ByteString uuid) {
    Supplier<ImportEngineRequest> request =
        () -> ImportEngineRequest.newBuilder().setPdAddr(pdAddr).setUuid(uuid).build();

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
  }

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
