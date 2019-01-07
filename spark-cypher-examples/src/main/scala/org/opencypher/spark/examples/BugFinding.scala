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

import org.apache.spark.sql.{DataFrame, functions}
import org.opencypher.okapi.api.configuration.Configuration.PrintDebug
import org.opencypher.okapi.api.graph.GraphName
import org.opencypher.okapi.logical.api.configuration.LogicalConfiguration.PrintLogicalPlan
import org.opencypher.okapi.relational.api.configuration.CoraConfiguration.PrintRelationalPlan
import org.opencypher.okapi.relational.impl.graph.ScanGraph
import org.opencypher.spark.api.io.sql.IdGenerationStrategy
import org.opencypher.spark.api.{CAPSSession, GraphSources}
import org.opencypher.spark.impl.table.SparkTable.DataFrameTable
import org.opencypher.spark.util.{ConsoleApp, HiveSetupForDebug}

object JoinBug extends ConsoleApp {

  implicit val session = CAPSSession.local().sparkSession

  import session.sqlContext.implicits._

  val nodes = Seq(
    (1L),
    (2L),
    (0L),
    (3L)
  ).toDF( "node_property_CUSTOMERIDX").withColumn("target", functions.monotonically_increasing_id())

  val edges = Seq(
    (0L, 0L),
    (1L, 0L),
    (2L, 0L),
    (3L, 0L),
    (4L, 0L),
    (5L, 0L),
    (6L, 1L),
    (7L, 1L),
    (8L, 1L),
    (9L, 1L),
    (10L, 2L),
    (11L, 2L),
    (12L, 2L),
    (13L, 2L),
    (14L, 3L),
    (15L, 3L),
    (16L, 3L),
    (17L, 3L)
  ).toDF("edge_property_interactionId", "edge_property_customerIdx")
    .withColumn("edge_id", functions.monotonically_increasing_id())
    .withColumn("edge_source", functions.monotonically_increasing_id() + 100)

  val leftToRight: DataFrame = nodes.join(edges, nodes.col("node_property_CUSTOMERIDX") === edges.col("edge_property_CUSTOMERIDX"))
  val sortedCols = leftToRight.columns.sorted.toSeq
  leftToRight.select(sortedCols.head, sortedCols.tail: _*).orderBy("edge_id").show()
  val rightToLeft: DataFrame = edges.join(nodes, nodes.col("node_property_CUSTOMERIDX") === edges.col("edge_property_CUSTOMERIDX"))
  rightToLeft.select(sortedCols.head, sortedCols.tail: _*).orderBy("edge_id").show()

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
