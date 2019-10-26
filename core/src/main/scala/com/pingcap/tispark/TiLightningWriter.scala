package com.pingcap.tispark
import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, TiContext}

class TiLightningWriter {
  def write(df: DataFrame,
            sqlContext: SQLContext,
            saveMode: SaveMode,
            options: TiDBOptions): Unit = {
    val tiContext = new TiContext(sqlContext.sparkSession, Some(options))
    val conn = TiDBUtils.createConnectionFactory(options.url)()

    try {
      val tableExists = TiDBUtils.tableExists(conn, options)
      if (tableExists) {
        TiLightningWrite.writeToTiDB(df, tiContext, options)
      } else {
        throw new TiBatchWriteException(
          s"table `${options.database}`.`${options.table}` does not exists!"
        )
        // TiDBUtils.createTable(conn, df, options, tiContext)
        // TiDBUtils.saveTable(tiContext, df, Some(df.schema), options)
      }
    } finally {
      conn.close()
    }
  }
}
