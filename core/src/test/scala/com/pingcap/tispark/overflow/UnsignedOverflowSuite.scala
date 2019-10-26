package com.pingcap.tispark.overflow

import com.pingcap.tispark.datasource.BaseDataSourceTest
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/**
 * UNSIGNED type include:
 * 1. TINYINT UNSIGNED
 * 2. SMALLINT UNSIGNED
 * 3. MEDIUMINT UNSIGNED
 * 4. INT UNSIGNED
 * 5. BIGINT UNSIGNED
 */
class UnsignedOverflowSuite extends BaseDataSourceTest("test_data_type_unsigned_overflow") {

  test("Test TINYINT UNSIGNED Upper bound Overflow") {
    testTinyIntUnsignedUpperBound(false)
  }

  test("Test TINYINT UNSIGNED as key Upper bound Overflow") {
    testTinyIntUnsignedUpperBound(true)
  }

  private def testTinyIntUnsignedUpperBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 TINYINT UNSIGNED primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 TINYINT UNSIGNED)"
      )
    }

    val row = Row(256)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 256 > upperBound 255"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test TINYINT UNSIGNED Lower bound Overflow") {
    testTinyIntUnsignedLowerBound(false)
  }

  test("Test TINYINT UNSIGNED as key Lower bound Overflow") {
    testTinyIntUnsignedLowerBound(false)
  }

  private def testTinyIntUnsignedLowerBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 TINYINT UNSIGNED primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 TINYINT UNSIGNED)"
      )
    }

    val row = Row(-1)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -1 < lowerBound 0"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test SMALLINT UNSIGNED Upper bound Overflow") {
    testSmallIntUnsignedUpperBound(false)
  }

  test("Test SMALLINT UNSIGNED as key Upper bound Overflow") {
    testSmallIntUnsignedUpperBound(true)
  }

  private def testSmallIntUnsignedUpperBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 SMALLINT UNSIGNED primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 SMALLINT UNSIGNED)"
      )
    }

    val row = Row(65536)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 65536 > upperBound 65535"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test SMALLINT UNSIGNED Lower bound Overflow") {
    testSmallIntUnsignedLowerBound(false)
  }

  test("Test SMALLINT UNSIGNED as key Lower bound Overflow") {
    testSmallIntUnsignedLowerBound(true)
  }

  private def testSmallIntUnsignedLowerBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 SMALLINT UNSIGNED primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 SMALLINT UNSIGNED)"
      )
    }

    val row = Row(-1)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -1 < lowerBound 0"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test MEDIUMINT UNSIGNED Upper bound Overflow") {
    testMediumIntUnsignedUpperBound(false)
  }

  test("Test MEDIUMINT UNSIGNED as key Upper bound Overflow") {
    testMediumIntUnsignedUpperBound(true)
  }

  private def testMediumIntUnsignedUpperBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 MEDIUMINT UNSIGNED primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 MEDIUMINT UNSIGNED)"
      )
    }

    val row = Row(16777216)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 16777216 > upperBound 16777215"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test MEDIUMINT UNSIGNED Lower bound Overflow") {
    testMediumIntUnsignedLowerBound(false)
  }

  test("Test MEDIUMINT UNSIGNED as key Lower bound Overflow") {
    testMediumIntUnsignedLowerBound(true)
  }

  private def testMediumIntUnsignedLowerBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 MEDIUMINT UNSIGNED primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 MEDIUMINT UNSIGNED)"
      )
    }

    val row = Row(-1)
    val schema = StructType(
      List(
        StructField("c1", IntegerType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value -1 < lowerBound 0"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test INT UNSIGNED Upper bound Overflow") {
    testIntUnsignedUpperBound(false)
  }

  test("Test INT UNSIGNED as key Upper bound Overflow") {
    testIntUnsignedUpperBound(true)
  }

  private def testIntUnsignedUpperBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 INT UNSIGNED primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 INT UNSIGNED)"
      )
    }

    val row = Row(4294967296L)
    val schema = StructType(
      List(
        StructField("c1", LongType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[com.pingcap.tikv.exception.ConvertOverflowException]
    val tidbErrorMsg = "value 4294967296 > upperBound 4294967295"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test INT UNSIGNED Lower bound Overflow") {
    testIntUnsignedLowerBound(false)
  }

  test("Test INT UNSIGNED as key Lower bound Overflow") {
    testIntUnsignedLowerBound(true)
  }

  private def testIntUnsignedLowerBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 INT UNSIGNED primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 INT UNSIGNED)"
      )
    }

    val row = Row(-1)
    val schema = StructType(
      List(
        StructField("c1", LongType)
      )
    )
    val jdbcErrorClass = classOf[java.lang.RuntimeException]
    val jdbcErrorMsgStartWith =
      "Error while encoding: java.lang.RuntimeException: java.lang.Integer is not a valid external type for schema of bigint\nif (assertnotnull(input[0, org.apache.spark.sql.Row, true]).isNullAt) null else validateexternaltype(getexternalrowfield(assertnotnull(input[0, org.apache.spark.sql.Row, true]), 0, c1), LongType) AS c1"
    val tidbErrorClass = classOf[java.lang.RuntimeException]
    val tidbErrorMsgStartWith =
      "Error while encoding: java.lang.RuntimeException: java.lang.Integer is not a valid external type for schema of bigint\nif (assertnotnull(input[0, org.apache.spark.sql.Row, true]).isNullAt) null else validateexternaltype(getexternalrowfield(assertnotnull(input[0, org.apache.spark.sql.Row, true]), 0, c1), LongType) AS c1"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsgStartWith,
      tidbErrorClass,
      tidbErrorMsgStartWith,
      msgStartWith = true
    )
  }

  test("Test BIGINT UNSIGNED Upper bound Overflow") {
    testBigIntUnsignedUpperBound(false)
  }

  test("Test BIGINT UNSIGNED as key Upper bound Overflow") {
    testBigIntUnsignedUpperBound(true)
  }

  private def testBigIntUnsignedUpperBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BIGINT UNSIGNED primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BIGINT UNSIGNED)"
      )
    }

    val row = Row("18446744073709551616")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[java.lang.NumberFormatException]
    val tidbErrorMsg = "Too large for unsigned long: 18446744073709551616"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  test("Test BIGINT UNSIGNED Lower bound Overflow") {
    testBigIntUnsignedLowerBound(false)
  }

  test("Test BIGINT UNSIGNED as key Lower bound Overflow") {
    testBigIntUnsignedLowerBound(true)
  }

  private def testBigIntUnsignedLowerBound(testKey: Boolean): Unit = {
    dropTable()
    if (testKey) {
      jdbcUpdate(
        s"create table $dbtable(c1 BIGINT UNSIGNED primary key)"
      )
    } else {
      jdbcUpdate(
        s"create table $dbtable(c1 BIGINT UNSIGNED)"
      )
    }

    val row = Row("-1")
    val schema = StructType(
      List(
        StructField("c1", StringType)
      )
    )
    val jdbcErrorClass = classOf[java.sql.BatchUpdateException]
    val jdbcErrorMsg = "Data truncation: Out of range value for column 'c1' at row 1"
    val tidbErrorClass = classOf[java.lang.NumberFormatException]
    val tidbErrorMsg = "-1"

    compareTiDBWriteFailureWithJDBC(
      List(row),
      schema,
      jdbcErrorClass,
      jdbcErrorMsg,
      tidbErrorClass,
      tidbErrorMsg
    )
  }

  override def afterAll(): Unit =
    try {
      dropTable()
    } finally {
      super.afterAll()
    }
}
