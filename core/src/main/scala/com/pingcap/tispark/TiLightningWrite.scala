package com.pingcap.tispark

import org.apache.spark.sql.{DataFrame, TiContext}

object TiLightningWrite {
  def writeToTiDB(df: DataFrame, tiContext: TiContext, options: TiDBOptions): Unit = ???

}
