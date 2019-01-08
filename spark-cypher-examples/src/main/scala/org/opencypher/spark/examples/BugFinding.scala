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
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.util.ConsoleApp

object JoinBug extends ConsoleApp {

  val databaseName = "customers"
  val baseTableName = s"$databaseName.csv_input"

  implicit val session = CAPSSession.local()

  import session.sparkSession.sqlContext.implicits._

  val datafile = getClass.getResource("/customer-interactions/csv/debug.csv").toURI.getPath

  val baseTable: DataFrame = session.sparkSession.read
    .format("csv")
    .option("header", "true")
    .load(datafile)

  val customers = baseTable.select("customerIdx", "customerId", "customerName").distinct

//  val customers = Seq(
//    (1, "W9VU80OL52R", "Neta Whinnery"),
//    (2, "W9VU80OL52R", "Neta Whinnery"),
//    (0, "X5CO34AD98M", "Elsa Clukey"),
//    (3, "R8RY34BW05P", "Wei Rolfe")
//  ).toDF("customerIdx", "customerId", "customerName")

//  val nodes = session.sparkSession.sql(s"SELECT * FROM customers.customers_SEED")
//      .withColumn("id", functions.monotonically_increasing_id() + 0L)
//      .withColumnRenamed("id", "target")

  val nodes = customers
    .withColumn("id", functions.monotonically_increasing_id() + 0L)
    .withColumnRenamed("id", "target")

  nodes.show()
  nodes.explain(true)

  val edges = Seq(
    (14L, 3L),
    (15L, 3L),
    (16L, 3L),
    (17L, 3L)
  ).toDF("edge_property_interactionId", "edge_property_customerIdx")
    .withColumn("edge_id", functions.monotonically_increasing_id())
    .withColumn("edge_source", functions.monotonically_increasing_id() + 100)

  val leftToRight: DataFrame = nodes.join(edges, nodes.col("customerIdx") === edges.col("edge_property_customerIdx"))
  val sortedCols = leftToRight.columns.sorted.toSeq
  leftToRight.select(sortedCols.head, sortedCols.tail: _*).orderBy("edge_id").show()
  val rightToLeft: DataFrame = edges.join(nodes, nodes.col("customerIdx") === edges.col("edge_property_customerIdx"))
  rightToLeft.select(sortedCols.head, sortedCols.tail: _*).orderBy("edge_id").show()

}
