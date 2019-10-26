package com.pingcap.tispark.datasource

import com.pingcap.tikv.exception.TiBatchWriteException
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

// TODO: once exception message is stable, we need also check the exception's message.
class AddingIndexInsertSuite extends BaseDataSourceTest("adding_index_insert") {
  private val row1 = Row(1, 1, "Hello")
  private val row2 = Row(2, 2, "TiDB")
  private val row3 = Row(3, 3, "Spark")
  private val row4 = Row(4, 4, "abde")
  private val row5 = Row(5, 5, "Duplicate")

  private val schema = StructType(
    List(
      StructField("pk", IntegerType),
      StructField("i", IntegerType),
      StructField("s", StringType)
    )
  )

  test("test column type can be truncated") {
    dropTable()
    jdbcUpdate(
      s"create table $dbtable(pk int, i int, s varchar(128), unique index(s(2)))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(1, 1, 'Hello')"
    )
    // insert row2 row3
    tidbWrite(List(row2, row3), schema)
    testTiDBSelect(Seq(row1, row2, row3))

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWrite(List(row2, row4), schema)
    }
  }

  test("no pk, no unique index case") {
    dropTable()
    jdbcUpdate(
      s"create table $dbtable(pk int, i int, s varchar(128), index(i))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(1, 1, 'Hello')"
    )
    // insert row2 row3
    tidbWrite(List(row2, row3), schema)
    testTiDBSelect(Seq(row1, row2, row3))

    tidbWrite(List(row2, row4), schema)
    testTiDBSelect(Seq(row1, row2, row2, row3, row4))

    tidbWrite(List(row4, row4), schema)
    testTiDBSelect(Seq(row1, row2, row2, row3, row4, row4, row4))
  }

  test("pk is not handle adding unique index") {
    dropTable()
    jdbcUpdate(
      s"create table $dbtable(pk int, i int, s varchar(128), unique index(i), primary key(s))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(1, 1, 'Hello')"
    )
    // insert row2 row3
    tidbWrite(List(row2, row3), schema)
    testTiDBSelect(Seq(row1, row2, row3))

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWrite(List(row2, row4), schema)
    }
  }

  test("pk is handle adding unique index") {
    dropTable()
    jdbcUpdate(
      s"create table $dbtable(pk int, i int, s varchar(128), unique index(i), primary key(pk))"
    )
    jdbcUpdate(
      s"insert into $dbtable values(1, 1, 'Hello')"
    )
    // insert row2 row3
    tidbWrite(List(row2, row3), schema)
    testTiDBSelect(Seq(row1, row2, row3))

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWrite(List(row2, row4), schema)
    }
  }

  test("Test no pk adding unique index") {
    dropTable()
    jdbcUpdate(s"create table $dbtable(pk int, i int, s varchar(128), unique index(i))")
    jdbcUpdate(
      s"insert into $dbtable values(1, 1, 'Hello')"
    )

    // insert row2 row3
    tidbWrite(List(row2, row3), schema)
    testTiDBSelect(Seq(row1, row2, row3))

    intercept[TiBatchWriteException] {
      // insert row2 row4
      tidbWrite(List(row2, row4), schema)
    }
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
