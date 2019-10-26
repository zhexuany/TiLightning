package com.pingcap.tispark

import java.util.UUID

import com.google.protobuf.ByteString
import com.pingcap.tikv.{TiConfiguration, TiSession}
import com.pingcap.tikv.lightning.ImportKVClient
import com.pingcap.tikv.meta.{TiColumnInfo, TiDBInfo, TiTableInfo}
import com.pingcap.tikv.row.ObjectRowImpl
import com.pingcap.tispark.TiLightningWrite.{SparkRow, TiRow}
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException
import org.apache.spark.sql.{DataFrame, TiContext}

import scala.collection.JavaConverters._

object TiLightningWrite {
  type SparkRow = org.apache.spark.sql.Row
  type TiRow = com.pingcap.tikv.row.Row
  type TiDataType = com.pingcap.tikv.types.DataType

  def write(df: DataFrame, tiContext: TiContext, options: TiDBOptions): Unit = {
    new TiLightningWrite(df, tiContext, options).write()
  }
}

class TiLightningWrite(df: DataFrame, tiContext: TiContext, options: TiDBOptions) {

  private var tiConf: TiConfiguration = _
  @transient private var tiSession: TiSession = _

  private var tiTableRef: TiTableReference = _
  private var tiDBInfo: TiDBInfo = _
  private var tiTableInfo: TiTableInfo = _

  private var tableColSize: Int = _

  private var colsMapInTiDB: Map[String, TiColumnInfo] = _
  private var colsInDf: List[String] = _

  private def write(): Unit = {
    try {
      // TODO: switch to import mode
      doWrite()
    } finally {
      close()
    }
  }

  def close(): Unit = {}

  private def doWrite(): Unit = {
    // initialize
    tiConf = tiContext.tiConf
    tiSession = tiContext.tiSession
    tiTableRef = options.tiTableRef
    tiDBInfo = tiSession.getCatalog.getDatabase(tiTableRef.databaseName)
    tiTableInfo = tiSession.getCatalog.getTable(tiTableRef.databaseName, tiTableRef.tableName)

    // TODO: remove this check?
    if (tiTableInfo == null) {
      throw new NoSuchTableException(tiTableRef.databaseName, tiTableRef.tableName)
    }

    colsMapInTiDB = tiTableInfo.getColumns.asScala.map(col => col.getName -> col).toMap
    colsInDf = df.columns.toList.map(_.toLowerCase())

    // spark row -> tikv row
    val tiRowRdd = df.rdd.map(row => sparkRow2TiKVRow(row))

    // TODO: split by region @zhexuany
    tiRowRdd.foreachPartition(iter => doImport(iter));
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

  private def doImport(iter: Iterator[TiRow]): Unit = {
    val importKVClient = new ImportKVClient()
    val uuid = UUID.randomUUID()
    // import engine is open
    importKVClient.openEngine(ByteString.copyFrom(uuid.toString.getBytes))

    // TODO: restore engine -> import engine -> import kv -> close engine -> clean up
  }
}
