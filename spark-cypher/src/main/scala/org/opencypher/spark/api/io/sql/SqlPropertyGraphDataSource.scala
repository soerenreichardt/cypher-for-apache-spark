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
package org.opencypher.spark.api.io.sql

import org.apache.spark.sql.{DataFrame, functions}
import org.opencypher.graphddl.GraphDdl.PropertyMappings
import org.opencypher.graphddl._
import org.opencypher.okapi.api.graph.{GraphName, PropertyGraph}
import org.opencypher.okapi.api.io.conversion.{EntityMapping, NodeMapping, RelationshipMapping}
import org.opencypher.okapi.impl.exception.{GraphNotFoundException, IllegalArgumentException, UnsupportedOperationException}
import org.opencypher.okapi.impl.util.StringEncodingUtilities._
import org.opencypher.spark.api.CAPSSession
import org.opencypher.spark.api.io.AbstractPropertyGraphDataSource._
import org.opencypher.spark.api.io.GraphEntity.sourceIdKey
import org.opencypher.spark.api.io.Relationship.{sourceEndNodeKey, sourceStartNodeKey}
import org.opencypher.spark.api.io._
import org.opencypher.spark.api.io.sql.IdGenerationStrategy._
import org.opencypher.spark.impl.DataFrameOps._
import org.opencypher.spark.impl.io.CAPSPropertyGraphDataSource
import org.opencypher.spark.schema.CAPSSchema
import org.opencypher.spark.schema.CAPSSchema._

case class SqlPropertyGraphDataSource(
  graphDdl: GraphDdl,
  sqlDataSourceConfigs: List[SqlDataSourceConfig],
  idGenerationStrategy: IdGenerationStrategy = MonotonicallyIncreasingId
)(implicit val caps: CAPSSession) extends CAPSPropertyGraphDataSource {

  override def hasGraph(graphName: GraphName): Boolean = graphDdl.graphs.contains(graphName)

  override def graph(graphName: GraphName): PropertyGraph = {

    val ddlGraph = graphDdl.graphs.getOrElse(graphName, throw GraphNotFoundException(s"Graph $graphName not found"))
    val capsSchema = ddlGraph.graphType

    // Build CAPS node tables
    val nodeDataFrames = ddlGraph.nodeToViewMappings.mapValues(nvm => readSqlTable(nvm.view))

    // Generate node identifiers
    val nodeDataFramesWithIds = idGenerationStrategy match {
      case HashBasedId => createHashIdForTables(nodeDataFrames, ddlGraph, sourceIdKey)
      case MonotonicallyIncreasingId => createMonotonicallyIncreasingIdForTables(nodeDataFrames, sourceIdKey)
    }

    val nodeTables = nodeDataFramesWithIds.map {
      case (nodeViewKey, nodeDf) =>
        val nodeType = nodeViewKey.nodeType
        val columnsWithType = nodeColsWithCypherType(capsSchema, nodeType)
        val inputNodeMapping = createNodeMapping(nodeType, ddlGraph.nodeToViewMappings(nodeViewKey).propertyMappings)
        val normalizedDf = normalizeDataFrame(nodeDf, inputNodeMapping)
        val normalizedMapping = normalizeNodeMapping(inputNodeMapping)

        val validatedDf = normalizedDf
          .validateColumnTypes(columnsWithType)
          .setNullability(columnsWithType)

        CAPSNodeTable.fromMapping(normalizedMapping, validatedDf)
    }.toSeq

    // Build CAPS relationship tables
    val relDataFrames = ddlGraph.edgeToViewMappings.map(evm => evm.key -> readSqlTable(evm.view)).toMap

    // Generate relationship identifiers
    val relDataFramesWithIds = idGenerationStrategy match {
      case HashBasedId => createHashIdForTables(relDataFrames, ddlGraph, sourceIdKey)
      case MonotonicallyIncreasingId => createMonotonicallyIncreasingIdForTables(relDataFrames, sourceIdKey)
    }

    val relationshipTables = ddlGraph.edgeToViewMappings.map { edgeToViewMapping =>
      val edgeViewKey = edgeToViewMapping.key
      val relType = edgeViewKey.edgeType.head
      val relDf = relDataFramesWithIds(edgeViewKey)
      val startNodeViewKey = edgeToViewMapping.startNode.nodeViewKey
      val endNodeViewKey = edgeToViewMapping.endNode.nodeViewKey

      val relsWithStartNodeId = idGenerationStrategy match {
        case HashBasedId =>
          // generate the start node id using the same hash parameters as for the corresponding node table
          val idColumnNames = edgeToViewMapping.startNode.joinPredicates.map(_.edgeColumn).map(_.toPropertyColumnName)
          createHashIdForTable(relDf, startNodeViewKey, idColumnNames, sourceStartNodeKey)
        case MonotonicallyIncreasingId =>
          val startNodeDf = nodeDataFramesWithIds(startNodeViewKey)
          joinNodeAndEdgeDf(startNodeDf, relDf, edgeToViewMapping.startNode.joinPredicates, sourceStartNodeKey)
      }

      val relsWithEndNodeId = idGenerationStrategy match {
        case HashBasedId =>
          // generate the end node id using the same hash parameters as for the corresponding node table
          val idColumnNames = edgeToViewMapping.endNode.joinPredicates.map(_.edgeColumn).map(_.toPropertyColumnName)
          createHashIdForTable(relsWithStartNodeId, endNodeViewKey, idColumnNames, sourceEndNodeKey)
        case MonotonicallyIncreasingId =>
          val endNodeDf = nodeDataFramesWithIds(endNodeViewKey)
          joinNodeAndEdgeDf(endNodeDf, relsWithStartNodeId, edgeToViewMapping.endNode.joinPredicates, sourceEndNodeKey)
      }

      val columnsWithType = relColsWithCypherType(capsSchema, relType)
      val inputRelMapping = createRelationshipMapping(relType, edgeToViewMapping.propertyMappings)
      val normalizedDf = normalizeDataFrame(relsWithEndNodeId, inputRelMapping)
      val normalizedMapping = normalizeRelationshipMapping(inputRelMapping)

      val validatedDf = normalizedDf
        .validateColumnTypes(columnsWithType)
        .setNullability(columnsWithType)

      CAPSRelationshipTable.fromMapping(normalizedMapping, validatedDf)
    }

    caps.graphs.create(nodeTables.head, nodeTables.tail ++ relationshipTables: _*)
  }

  private def joinNodeAndEdgeDf(
    nodeDf: DataFrame,
    edgeDf: DataFrame,
    joinColumns: List[Join],
    newNodeIdColumn: String
  ): DataFrame = {

    val nodePrefix = "node_"
    val edgePrefix = "edge_"

    // to avoid collisions on column names
    val namespacedNodeDf = nodeDf.prefixColumns(nodePrefix)
    val namespacedEdgeDf = edgeDf.prefixColumns(edgePrefix)

    val namespacedJoinColumns = joinColumns
      .map(join => Join(nodePrefix + join.nodeColumn.toPropertyColumnName, edgePrefix + join.edgeColumn.toPropertyColumnName))

    val joinPredicate = namespacedJoinColumns
      .map(join => namespacedNodeDf.col(join.nodeColumn) === namespacedEdgeDf.col(join.edgeColumn))
      .reduce(_ && _)

    val nodeIdColumnName = nodePrefix + sourceIdKey

    // attach id from nodes as start/end by joining on the given columns
    val edgeDfWithNodesJoined = namespacedNodeDf
      .select(nodeIdColumnName, namespacedJoinColumns.map(_.nodeColumn): _*)
      .withColumnRenamed(nodeIdColumnName, newNodeIdColumn)
      .join(namespacedEdgeDf, joinPredicate)

    // drop unneeded node columns (those we joined on) and drop namespace on edge columns
    edgeDfWithNodesJoined.columns.foldLeft(edgeDfWithNodesJoined) {
      case (currentDf, columnName) if columnName.startsWith(nodePrefix) =>
        currentDf.drop(columnName)
      case (currentDf, columnName) if columnName.startsWith(edgePrefix) =>
        currentDf.withColumnRenamed(columnName, columnName.substring(edgePrefix.length))
      case (currentDf, _) =>
        currentDf
    }
  }

  private def readSqlTable(qualifiedViewId: QualifiedViewId): DataFrame = {
    val spark = caps.sparkSession

    val sqlDataSourceConfig = sqlDataSourceConfigs.find(_.dataSourceName == qualifiedViewId.dataSource).get
    val tableName = qualifiedViewId.schema + "." + qualifiedViewId.view
    val inputTable = sqlDataSourceConfig.storageFormat match {
      case JdbcFormat =>
        spark.read
          .format("jdbc")
          .option("url", sqlDataSourceConfig.jdbcUri.getOrElse(throw SqlDataSourceConfigException("Missing JDBC URI")))
          .option("driver", sqlDataSourceConfig.jdbcDriver.getOrElse(throw SqlDataSourceConfigException("Missing JDBC Driver")))
          .option("fetchSize", sqlDataSourceConfig.jdbcFetchSize)
          .option("dbtable", tableName)
          .load()

      case HiveFormat =>

        spark.table(tableName)

      case otherFormat => notFound(otherFormat, Seq(JdbcFormat, HiveFormat))
    }

    inputTable.withPropertyColumns
  }

  private def normalizeDataFrame(dataFrame: DataFrame, mapping: EntityMapping): DataFrame = {
    val dfColumns = dataFrame.schema.fieldNames.map(_.toLowerCase).toSet

    mapping.propertyMapping.foldLeft(dataFrame) {
      case (currentDf, (property, column)) if dfColumns.contains(column.toLowerCase) =>
        currentDf.withColumnRenamed(column, property.toPropertyColumnName)
      case (_, (_, column)) => throw IllegalArgumentException(
        expected = s"Column with name $column",
        actual = dfColumns
      )
    }
  }

  private def normalizeNodeMapping(mapping: NodeMapping): NodeMapping = {
    createNodeMapping(mapping.impliedLabels, mapping.propertyMapping.keys.map(key => key -> key).toMap)
  }

  private def normalizeRelationshipMapping(mapping: RelationshipMapping): RelationshipMapping = {
    createRelationshipMapping(mapping.relTypeOrSourceRelTypeKey.left.get, mapping.propertyMapping.keys.map(key => key -> key).toMap)
  }

  private def createNodeMapping(labelCombination: Set[String], propertyMappings: PropertyMappings): NodeMapping = {
    val initialNodeMapping = NodeMapping.on(sourceIdKey).withImpliedLabels(labelCombination.toSeq: _*)
    propertyMappings.foldLeft(initialNodeMapping) {
      case (currentNodeMapping, (propertyKey, columnName)) =>
        currentNodeMapping.withPropertyKey(propertyKey -> columnName.toPropertyColumnName)
    }
  }

  private def createRelationshipMapping(
    relType: String,
    propertyMappings: PropertyMappings
  ): RelationshipMapping = {
    val initialRelMapping = RelationshipMapping.on(sourceIdKey)
      .withSourceStartNodeKey(sourceStartNodeKey)
      .withSourceEndNodeKey(sourceEndNodeKey)
      .withRelType(relType)
    propertyMappings.foldLeft(initialRelMapping) {
      case (currentRelMapping, (propertyKey, columnName)) =>
        currentRelMapping.withPropertyKey(propertyKey -> columnName.toPropertyColumnName)
    }
  }

  /**
    * Creates a potentially unique 64-bit identifier for each row in the given input table. The identifier is computed
    * by hashing the view name, the element type (i.e. its labels) and the values stored in a given set of columns.
    *
    * @param dataFrame input table / view
    * @param elementViewKey node / edge view key used for hashing
    * @param idColumnNames columns used for hashing
    * @param newIdColumn name of the new id column
    * @tparam T node / edge view key
    * @return input table / view with an additional column that contains potentially unique identifiers
    */
  private def createHashIdForTable[T <: ElementViewKey](
    dataFrame: DataFrame,
    elementViewKey: T,
    idColumnNames: List[String],
    newIdColumn: String
  ): DataFrame = {
    val viewLiteral = functions.lit(elementViewKey.qualifiedViewId.view)
    val elementTypeLiterals = elementViewKey.elementType.toSeq.sorted.map(functions.lit)
    val idColumns = idColumnNames.map(dataFrame.col)
    dataFrame.withHashColumn(Seq(viewLiteral) ++ elementTypeLiterals ++ idColumns, newIdColumn)
  }

  /**
    * Creates a potentially unique 64-bit identifier for each row in the given input tables. The identifier is computed
    * by hashing a specific set of columns of the input table. For node tables, we either pick the the join columns from
    * the relationship mappings (i.e. the columns we join on) or all columns if the node is unconnected.
    *
    * In order to reduce the probability of hash collisions, the view name and the element type (i.e. its labels) are
    * additional input for the hash function.
    *
    * @param views input tables
    * @param ddlGraph DDL graph instance definition
    * @param newIdColumn name of the new id column
    * @tparam T node / edge view key
    * @return input tables with an additional column that contains potentially unique identifiers
    */
  private def createHashIdForTables[T <: ElementViewKey](
    views: Map[T, DataFrame],
    ddlGraph: Graph,
    newIdColumn: String
  ): Map[T, DataFrame] = {
    views.map { case (elementViewKey, dataFrame) =>
      val idColumnNames = elementViewKey match {
        case nvk: NodeViewKey => ddlGraph.nodeIdColumnsFor(nvk) match {
          case Some(columnNames) => columnNames.map(_.toPropertyColumnName)
          case None => dataFrame.columns.toList
        }
        case _: EdgeViewKey => dataFrame.columns.toList
      }
      elementViewKey -> createHashIdForTable(dataFrame, elementViewKey, idColumnNames, newIdColumn)
    }
  }

  /**
    * Creates a unique 64-bit identifier for each row in the given input tables. The identifier is monotonically
    * increasing across the input tables.
    *
    * @param views input tables
    * @param newIdColumn name of the new id column
    * @tparam T node / edge view key
    * @return input tables with an additional column that contains unique identifiers
    */
  private def createMonotonicallyIncreasingIdForTables[T <: ElementViewKey](
    views: Map[T, DataFrame],
    newIdColumn: String
  ): Map[T, DataFrame] = {
    val (elementViewKeys, dataFrames) = views.unzip
    elementViewKeys.zip(addUniqueIds(dataFrames.toSeq, newIdColumn)).toMap
  }

  override def schema(name: GraphName): Option[CAPSSchema] = graphDdl.graphs.get(name).map(_.graphType.asCaps)

  override def store(name: GraphName, graph: PropertyGraph): Unit = unsupported("storing a graph")

  override def delete(name: GraphName): Unit = unsupported("deleting a graph")

  override def graphNames: Set[GraphName] = graphDdl.graphs.keySet

  private val className = getClass.getSimpleName

  private def unsupported(operation: String): Nothing =
    throw UnsupportedOperationException(s"$className does not allow $operation")

  private def notFound(needle: Any, haystack: Traversable[Any] = Traversable.empty): Nothing =
    throw IllegalArgumentException(
      expected = if (haystack.nonEmpty) s"one of ${stringList(haystack)}" else "",
      actual = needle
    )

  private def stringList(elems: Traversable[Any]): String =
    elems.mkString("[", ",", "]")
}
