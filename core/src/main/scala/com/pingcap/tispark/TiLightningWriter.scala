package com.pingcap.tispark
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, TiContext}

class TiLightningWriter {
  def write(df: DataFrame,
            sqlContext: SQLContext,
            saveMode: SaveMode,
            options: TiDBOptions): Unit = {
    val tiContext = new TiContext(sqlContext.sparkSession, Some(options))

    try {
      TiLightningWrite.write(df, tiContext, options)
    } catch {
      case e: Exception => throw new RuntimeException("lightning write failed: ", e)
    }
  }
}
