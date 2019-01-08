/*
 * Copyright (c) 2016-2018 "Neo4j Sweden, AB" [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Attribution Notice under the terms of the Apache License 2.0
 *
 * This work was created by the collective efforts of the openCypher community.
 * Without limiting the terms of Section 6, any Derivative Work that is not
 * approved by the public consensus process of the openCypher Implementers Group
 * should not be described as “Cypher” (and Cypher® is a registered trademark of
 * Neo4j Inc.) or as "openCypher". Extensions by implementers or prototypes or
 * proposals for change that have been documented or implemented should only be
 * described as "implementation extensions to Cypher" or as "proposed changes to
 * Cypher that are not yet approved by the openCypher community".
 */
package org.opencypher.spark.examples

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, functions}
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.spark.api.io.sql.IdGenerationStrategy
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.util.HiveSetupForDebug.{baseTableName, createView, databaseName, getClass}
import org.opencypher.spark.util.{ConsoleApp, HiveSetupForDebug}

object JoinBug extends ConsoleApp {

  val databaseName = "customers"
  val baseTableName = s"$databaseName.csv_input"

  implicit val session = CAPSSession.local()

  import session.sparkSession.sqlContext.implicits._

  val datafile = getClass.getResource("/customer-interactions/csv/debug.csv").toURI.getPath
  val structType = StructType(Seq(
    StructField("interactionId", LongType, nullable = false),
    StructField("date", StringType, nullable = false),
    StructField("customerIdx", LongType, nullable = false),
    StructField("empNo", LongType, nullable = false),
    StructField("empName", StringType, nullable = false),
    StructField("type", StringType, nullable = false),
    StructField("customerId", StringType, nullable = false),
    StructField("customerName", StringType, nullable = false)
  ))

  val baseTable: DataFrame = session.sparkSession.read
    .format("csv")
    .option("header", "true")
    .schema(structType)
    .load(datafile)

  session.sql(s"DROP DATABASE IF EXISTS $databaseName CASCADE")
  session.sql(s"CREATE DATABASE $databaseName")

  baseTable.write.saveAsTable(s"$baseTableName")

  // Create views for nodes
  createView(baseTableName, "interactions", true, "interactionId", "date", "type")
  createView(baseTableName, "customers", true, "customerIdx", "customerId", "customerName")
  createView(baseTableName, "customer_reps", true, "empNo", "empName")

  // Create views for relationships
  createView(baseTableName, "has_customer_reps", false, "interactionId", "empNo")
  createView(baseTableName, "has_customers", false, "interactionId", "customerIdx")

  val nodes = session.sparkSession.sql(s"SELECT * FROM $baseTableName").select(
    "customerIdx",
    "customerId",
    "customerName"
  )
      .distinct
      .withColumn("id", functions.monotonically_increasing_id() + 0L)
      .withColumnRenamed("id", "target")

  nodes.show()
  nodes.explain(true)

//  val nodes = Seq(
//    (1L),
//    (2L)
//  ).toDF( "node_customerIdx").withColumn("id", functions.monotonically_increasing_id())
//    .withColumnRenamed("id", "target")

//  val edges = Seq(
//    (6L, 1L),
//    (7L, 1L),
//    (8L, 1L)
//  ).toDF("interactionId", "rel_customerIdx")
//    .withColumn("edge_id", functions.monotonically_increasing_id())
//    .filter(functions.not(functions.isnull(functions.col("interactionId"))))
//
//  val leftToRight: DataFrame = nodes.join(edges, nodes.col("node_customerIdx") === edges.col("rel_customerIdx"))
//  val sortedCols = leftToRight.columns.sorted.toSeq
//  leftToRight.select(sortedCols.head, sortedCols.tail: _*).orderBy("edge_id").show()
//  val rightToLeft: DataFrame = edges.join(nodes, nodes.col("node_customerIdx") === edges.col("rel_customerIdx"))
//  rightToLeft.select(sortedCols.head, sortedCols.tail: _*).orderBy("edge_id").show()

}

object BugFinding extends ConsoleApp {

//  PrintDebug.set()

  implicit val session: CAPSSession = CAPSSession.local()

  // Load a CSV file of interactions into Hive tables (views)
  HiveSetupForDebug.load(false)

  val pgds = GraphSources
    .sql(file("/customer-interactions/ddl/debug.ddl"))
    .withIdGenerationStrategy(IdGenerationStrategy.MonotonicallyIncreasingId)
    .withSqlDataSourceConfigs(file("/customer-interactions/ddl/data-sources.json"))

  val graph = pgds.graph(GraphName("debug"))

//  graph.cypher(
//    """
//      |MATCH ()
//      |RETURN count(*) AS `nodeCount (26 is correct)`
//    """.stripMargin).show

  graph.cypher(
    """
      |MATCH ()-->()
      |RETURN count(*) AS `relCount (36 is correct)`
    """.stripMargin).show

//  graph.asInstanceOf[ScanGraph[DataFrameTable]].scans.foreach { scan =>
//    scan.table.show()
//  }

//  graph.cypher(
//    """
//      |MATCH ()-->()
//      |RETURN count(*) AS `relCount (36 is correct)`
//    """.stripMargin).records.asCaps.table.df.explain()

  session.sparkSession.close()


  def file(path: String): String = {
    getClass.getResource(path).toURI.getPath
  }
}
