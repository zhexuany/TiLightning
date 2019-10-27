package com.pingcap.tispark

import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}

import com.google.common.util.concurrent.ThreadFactoryBuilder
import com.pingcap.tikv.codec.{KeyUtils, TableCodec}
import com.pingcap.tikv.key.RowKey
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tikv.meta.{TiColumnInfo, TiDBInfo, TiTableInfo}
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tikv.util.UUIDType5
import com.pingcap.tispark.TiLightningWrite.{SparkRow, TiRow}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{DataFrame, TiContext}
import org.apache.spark.util.SizeEstimator
import org.slf4j.LoggerFactory
import org.tikv.kvproto.ImportKvpb.KVPair

import scala.collection.JavaConverters._

object TiLightningWrite {
  type SparkRow = org.apache.spark.sql.Row
  type TiRow = com.pingcap.tikv.row.Row
  type TiDataType = com.pingcap.tikv.types.DataType

  def write(df: DataFrame, tiContext: TiContext, options: TiDBOptions): Unit = {
    new TiLightningWrite(df, tiContext, options).write()
  }
}

class TiLightningWrite(@transient val df: DataFrame,
                       @transient val tiContext: TiContext,
                       options: TiDBOptions)
    extends Serializable {

  private final val logger = LoggerFactory.getLogger(getClass.getName)

  private var tiConf: TiConfiguration = _
  @transient private var tiSession: TiSession = _

  private var tiTableRef: TiTableReference = _
  private var tiDBInfo: TiDBInfo = _
  private var tiTableInfo: TiTableInfo = _

  private var colsMapInTiDB: Map[String, TiColumnInfo] = _
  private var colsInDf: List[String] = _

  @transient private val scheduledExecutorService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder().setDaemon(true).build);

  private def write(): Unit = {
    try {
      initialize()
      doWrite()
    } finally {
      close()
    }
  }

  def close(): Unit = {
    scheduledExecutorService.shutdownNow()
    switchToNormalMode()
  }

  private def makeTag(tableName: String, engineId: Long): String = s"$tableName:$engineId"

  private def initialize(): Unit = {
    // initialize
    tiConf = tiContext.tiConf
    tiSession = tiContext.tiSession
    tiTableRef = options.tiTableRef
    tiDBInfo = tiSession.getCatalog.getDatabase(tiTableRef.databaseName)
    tiTableInfo = tiSession.getCatalog.getTable(tiTableRef.databaseName, tiTableRef.tableName)

    switchToNormalMode()
    df.cache()

    scheduledExecutorService.scheduleAtFixedRate(new Runnable {
      override def run(): Unit = {
        switchToImportMode()
      }
    }, 0, 1, TimeUnit.SECONDS)
  }

  private def switchToImportMode(): Unit = {
    tiSession.getImportSSTClient.switchTiKVToImportMode()
  }

  private def switchToNormalMode(): Unit = {
    tiSession.getImportSSTClient.switchTiKVToNormalMode()
  }

  private def doWrite(): Unit = {
    // TODO: remove this check?
    if (tiTableInfo == null) {
      throw new NoSuchTableException(tiTableRef.databaseName, tiTableRef.tableName)
    }

    colsMapInTiDB = tiTableInfo.getColumns.asScala.map(col => col.getName -> col).toMap
    colsInDf = df.columns.toList.map(_.toLowerCase())

    // spark row -> tikv row
    val tiRowRdd: RDD[TiRow] = df.rdd.map(row => sparkRow2TiKVRow(row))

    // split and sort by row id
    val numOfRegions = splitRowByRegion(tiRowRdd)
    val rddAfterSplit: RDD[WrappedRow] = tiRowRdd
      .zipWithIndex()
      .sortBy(_._2, numPartitions = numOfRegions)
      .map(x => WrappedRow(x._1, x._2))

    // do import for each partition(region)
    rddAfterSplit.foreachPartition(iter => doImport(iter))
  }

  private def splitRowByRegion(rdd: RDD[TiRow]): Int = {
    val regionSize = 96 * 1024 * 1024;
    val SAMPLE_FRACTION = 0.005
    val totalSize = {
      val sampleRDD =
        rdd.sample(withReplacement = false, fraction = SAMPLE_FRACTION)
      val sampleRDDSize = getRDDSize(sampleRDD)
      sampleRDDSize / SAMPLE_FRACTION
    }
    Math.max(1, (totalSize / regionSize).toInt)
  }

  def getRDDSize(rdd: RDD[TiRow]): Long = {
    var rddSize = 0L
    val rows = rdd.collect()
    for (row <- rows) {
      rddSize += SizeEstimator.estimate(row)
    }

    rddSize
  }

  // convert spark's row to tikv row. We do not allocate handle for no pk case.
  // allocating handle id will be finished after we check conflict.
  private def sparkRow2TiKVRow(sparkRow: SparkRow): TiRow = {
    val fieldCount = sparkRow.size
    val tiRow = ObjectRowImpl.create(fieldCount)
    for (i <- 0 until fieldCount) {
      tiRow.set(
        colsMapInTiDB(colsInDf(i)).getOffset,
        null,
        colsMapInTiDB(colsInDf(i)).getType.convertToTiDBType(sparkRow(i))
      )
    }
    tiRow
  }

  /**
   * Open Engine -> Write -> Close Engine -> Import
   *
   * @param iter
   */
  private def doImport(iter: Iterator[WrappedRow]): Unit = {
    logger.info("entering doImport...")
    val tiSession = TiSession.getInstance(tiConf)
    val importKVClient = tiSession.getImportKVClient

    val tableName = tiTableInfo.getName
    if (!iter.hasNext) {
      logger.info("no rows involved.")
      return
    }
    val engineId = TaskContext.getPartitionId()
    val tag = makeTag(tableName, engineId)
    val uuid = UUIDType5
      .nameUUIDFromNamespaceAndString(UUID.fromString("d68d6abe-c59e-45d6-ade8-e2b0ceb7bedf"), tag)
    val uuid_s = UUIDType5.toBytes(uuid)

    // start OpenEngine
    logger.info("opening engine " + uuid)
    importKVClient.openEngine(uuid_s)
    logger.info("opened engine " + uuid)
    // TODO: restore engine -> import engine -> import kv -> close engine -> clean up
    // we skip restore engine to simplify logic

    val kvs = iter
      .map(x => generateRowKey(x.row, x.handle))
      .map(
        x =>
          KVPair
            .newBuilder()
            .setKey(KeyUtils.extract(x._1.bytes))
            .setValue(KeyUtils.extract(x._2))
            .build()
      )

    // write rows to kv
    logger.info("writing to kv via engine " + uuid)
    importKVClient.writeRowsV3(
      uuid_s,
      tableName,
      colsInDf.toArray,
      tiSession.getTimestamp.getVersion,
      kvs.toList.asJava
    )
    logger.info("wrote to kv via engine " + uuid)

    // close engine
    importKVClient.closeEngine(uuid_s)

    // start ImportEngine
    logger.info("importing engine " + uuid)
    importKVClient.importEngine(uuid_s)
    logger.info("imported engine " + uuid)

    // cleanup Engine
    logger.info("cleaning up engine " + uuid)
    importKVClient.cleanupEngine(uuid_s)
    logger.info("cleaned up engine " + uuid)

  }

  private def locatePhysicalTable(row: TiRow): Long = {
    tiTableInfo.getId
  }

  private def encodeTiRow(tiRow: TiRow): Array[Byte] = {
    val colSize = tiRow.fieldCount()

    val convertedValues = new Array[AnyRef](colSize)
    for (i <- 0 until colSize) {
      // pk is handle can be skipped
      val columnInfo = tiTableInfo.getColumn(i)
      val value = tiRow.get(i, columnInfo.getType)
      convertedValues.update(i, value)
    }

    TableCodec.encodeRow(tiTableInfo.getColumns, convertedValues, tiTableInfo.isPkHandle)
  }

  private def generateRowKey(row: TiRow, handle: Long): (SerializableKey, Array[Byte]) = {
    (
      new SerializableKey(RowKey.toRowKey(locatePhysicalTable(row), handle).getBytes),
      encodeTiRow(row)
    )
  }
}
