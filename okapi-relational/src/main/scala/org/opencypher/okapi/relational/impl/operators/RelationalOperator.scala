package org.opencypher.okapi.relational.impl.operators

import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue.CypherInteger
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, SchemaException, UnsupportedOperationException}
import org.opencypher.okapi.ir.api.block.{Asc, Desc, SortItem}
import org.opencypher.okapi.ir.api.expr.Expr._
import org.opencypher.okapi.ir.api.expr._
import org.opencypher.okapi.ir.api.{Label, RelType}
import org.opencypher.okapi.logical.impl.{LogicalCatalogGraph, LogicalPatternGraph}
import org.opencypher.okapi.relational.api.graph.{RelationalCypherGraph, RelationalCypherSession}
import org.opencypher.okapi.relational.api.physical.RelationalRuntimeContext
import org.opencypher.okapi.relational.api.schema.RelationalSchema._
import org.opencypher.okapi.relational.api.table.ExtractEntities.SelectExpressions
import org.opencypher.okapi.relational.api.table.{FlatRelationalTable, RelationalCypherRecords}
import org.opencypher.okapi.relational.impl.operators
import org.opencypher.okapi.relational.impl.physical._
import org.opencypher.okapi.relational.impl.table.RecordHeader
import org.opencypher.okapi.trees.AbstractTreeNode

abstract class RelationalOperator[T <: FlatRelationalTable[T]] extends AbstractTreeNode[RelationalOperator[T]] {

  type TagStrategy = Map[QualifiedGraphName, Map[Int, Int]]

  def tagStrategy: TagStrategy = Map.empty

  def header: RecordHeader = children.head.header

  def _table: T = children.head.table

  implicit def context: RelationalRuntimeContext[T] = children.head.context

  implicit def session: RelationalCypherSession[T] = context.session

  def graph: RelationalCypherGraph[T] = children.head.graph

  def graphName: QualifiedGraphName = children.head.graphName

  def returnItems: Option[Seq[Var]] = children.head.returnItems

  protected def resolve(qualifiedGraphName: QualifiedGraphName)
    (implicit context: RelationalRuntimeContext[T]): RelationalCypherGraph[T] =
    context.resolveGraph(qualifiedGraphName)

  protected def resolveTags(qgn: QualifiedGraphName)(implicit context: RelationalRuntimeContext[T]): Set[Int] =
    resolve(qgn).tags

  def table: T = {
    val t = _table

    if (t.physicalColumns.toSet != header.columns) {
      // Ensure no duplicate columns in initialData
      val initialDataColumns = t.physicalColumns

      val duplicateColumns = initialDataColumns.groupBy(identity).collect {
        case (key, values) if values.size > 1 => key
      }

      if (duplicateColumns.nonEmpty)
        throw IllegalArgumentException(
          s"${getClass.getSimpleName}: a table with distinct columns",
          s"a table with duplicate columns: ${initialDataColumns.sorted.mkString("[", ", ", "]")}")

      // Verify that all header column names exist in the data
      val headerColumnNames = header.columns
      val dataColumnNames = t.physicalColumns.toSet
      val missingTableColumns = headerColumnNames -- dataColumnNames
      if (missingTableColumns.nonEmpty) {
        throw IllegalArgumentException(
          s"${getClass.getSimpleName}: table with columns ${header.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
          s"""|table with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}
              |column(s) ${missingTableColumns.mkString(", ")} are missing in the table
           """.stripMargin
        )
      }
      // TODO: uncomment and fix expectations
      //      val missingHeaderColumns = dataColumnNames -- headerColumnNames
      //      if (missingHeaderColumns.nonEmpty) {
      //        throw IllegalArgumentException(
      //          s"data with columns ${header.columns.toSeq.sorted.mkString("\n[", ", ", "]\n")}",
      //          s"data with columns ${dataColumnNames.toSeq.sorted.mkString("\n[", ", ", "]\n")}"
      //        )
      //      }

      // Verify column types
      header.expressions.foreach { expr =>
        val tableType = t.columnType(header.column(expr))
        val headerType = expr.cypherType
        // if the type in the data doesn't correspond to the type in the header we fail
        // except: we encode nodes, rels and integers with the same data type, so we can't fail
        // on conflicts when we expect entities (alternative: change reverse-mapping function somehow)

        headerType match {
          case _: CTNode if tableType == CTInteger =>
          case _: CTNodeOrNull if tableType == CTInteger =>
          case _: CTRelationship if tableType == CTInteger =>
          case _: CTRelationshipOrNull if tableType == CTInteger =>
          case _ if tableType == headerType =>
          case _ => throw IllegalArgumentException(
            s"${getClass.getSimpleName}: data matching header type $headerType for expression $expr", tableType)
        }
      }
    }
    t
  }
}

// Leaf

object Start {

  def from[T <: FlatRelationalTable[T]](header: RecordHeader, table: T)(implicit context: RelationalRuntimeContext[T]): Start[T] = {
    Start(context.session.emptyGraphQgn, Some(context.session.records.from(header, table)))
  }

  def apply[T <: FlatRelationalTable[T]](records: RelationalCypherRecords[T])(implicit context: RelationalRuntimeContext[T]): Start[T] = {
    Start(context.session.emptyGraphQgn, Some(records))
  }
}

final case class Start[T <: FlatRelationalTable[T]](
  qgn: QualifiedGraphName,
  recordsOpt: Option[RelationalCypherRecords[T]] = None
)(implicit override val context: RelationalRuntimeContext[T]) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = recordsOpt.map(_.header).getOrElse(RecordHeader.empty)

  //  override lazy val _table: T = recordsOpt.map(_.table).getOrElse(CAPSRecords.unit().table)
  override lazy val _table: T = recordsOpt.map(_.table).getOrElse(table.unit)

  override lazy val graph: RelationalCypherGraph[T] = resolve(qgn)

  override lazy val graphName: QualifiedGraphName = qgn

  override lazy val returnItems: Option[Seq[Var]] = None

  override def toString: String = {
    val graphArg = qgn.toString
    val recordsArg = recordsOpt.map(_.toString)
    val allArgs = List(recordsArg, graphArg).mkString(", ")
    s"Start($allArgs)"
  }

}

// Unary

final case class Cache[T <: FlatRelationalTable[T]](in: RelationalOperator[T]) extends RelationalOperator[T] {

  override lazy val _table: T = context.cache.getOrElse(in, {
    in.table.cache
    context.cache += (in -> in.table)
    in.table
  })

}

final case class NodeScan[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  v: Var
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.graph.schema.headerForNode(v)

  // TODO: replace with NodeVar
  override lazy val _table: T = {
    val nodeTable = in.graph.nodes(v.name, v.cypherType.asInstanceOf[CTNode]).table

    if (header.columns != nodeTable.physicalColumns.toSet) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed columns based on graph schema: ${header.columns.toSeq.sorted.mkString(", ")}
           |  - Actual columns in scan table: ${nodeTable.physicalColumns.sorted.mkString(", ")}
        """.stripMargin)
    }
    nodeTable
  }
}

final case class RelationshipScan[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  v: Var
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.graph.schema.headerForRelationship(v)

  // TODO: replace with RelationshipVar
  override lazy val _table: T = {
    val relTable = in.graph.relationships(v.name, v.cypherType.asInstanceOf[CTRelationship]).table

    if (header.columns != relTable.physicalColumns.toSet) {
      throw SchemaException(
        s"""
           |Graph schema does not match actual records returned for scan $v:
           |  - Computed columns based on graph schema: ${header.columns.toSeq.sorted.mkString(", ")}
           |  - Actual columns in scan table: ${relTable.physicalColumns.sorted.mkString(", ")}
        """.stripMargin)
    }
    relTable
  }
}

final case class Alias[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  aliases: Seq[AliasExpr]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.header.withAlias(aliases: _*)
}

final case class Add[T <: FlatRelationalTable[T]](in: RelationalOperator[T], expr: Expr) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = {
    if (in.header.contains(expr)) {
      expr match {
        case a: AliasExpr => in.header.withAlias(a)
        case _ => in.header
      }
    } else {
      expr match {
        case a: AliasExpr => in.header.withExpr(a.expr).withAlias(a)
        case _ => in.header.withExpr(expr)
      }
    }
  }

  override lazy val _table: T = {
    // TODO check for equal nullability setting
    if (in.header.contains(expr)) {
      in.table
    } else {
      in.table.withColumn(header.column(expr), expr)(header, context.parameters)
    }
  }
}

final case class AddInto[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  add: Expr,
  into: Expr
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.header.withExpr(into)

  override lazy val _table: T = in.table.withColumn(header.column(into), add)(header, context.parameters)
}

final case class Drop[E <: Expr, T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  exprs: Set[E]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.header -- exprs

  private lazy val columnsToDrop = in.header.columns -- header.columns

  override lazy val _table: T = {
    if (columnsToDrop.nonEmpty) {
      in.table.drop(columnsToDrop.toSeq: _*)
    } else {
      in.table
    }
  }
}

final case class RenameColumns[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  renameExprs: Map[Expr, String]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = renameExprs.foldLeft(in.header) {
    case (currentHeader, (expr, newColumn)) => currentHeader.withColumnRenamed(expr, newColumn)
  }

  override lazy val _table: T = renameExprs.foldLeft(in.table) {
    case (currentTable, (expr, newColumn)) => currentTable.withColumnRenamed(in.header.column(expr), newColumn)
  }
}

final case class Filter[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  expr: Expr
) extends RelationalOperator[T] {

  override lazy val _table: T = in.table.filter(expr)(header, context.parameters)
}

final case class ReturnGraph[T <: FlatRelationalTable[T]](in: RelationalOperator[T]) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = RecordHeader.empty

  override lazy val _table: T = in.table.empty()
}

final case class Select[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  expressions: List[Expr]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.header.select(expressions: _*)

  override lazy val _table: T = {
    import Expr._
    val selectExpressions = expressions.map {
      case AliasExpr(_, alias) => alias
      case other => other
    }.flatMap(expr => header.expressionsFor(expr).toSeq.sorted)
    in.table.select(selectExpressions.map(header.column).distinct: _*)
  }

  override lazy val returnItems: Option[Seq[Var]] = Some(expressions.flatMap(_.owner).collect { case e: Var => e }.distinct)
}

object RelationalOperator {

  implicit class RelationalOperatorOps[T <: FlatRelationalTable[T]](val op: RelationalOperator[T]) extends AnyVal {

    // TODO: entity needs to contain all labels/relTypes: all case needs to be explicitly expanded with the schema
    def alignWith(entity: Var, targetHeader: RecordHeader): RelationalOperator[T] = {
      require(op.header.entityVars.size == 1,
        s"Align with only works on single entity tables, found ${op.header.entityVars}")

      val entityVar = op.header.entityVars.head

      // Rename variable and select only relevant expressions
      val relevantSelected = Select(op, List(entityVar as entity))

      // Fill in missing true label columns
      val trueLabels = entityVar.cypherType match {
        case CTNode(labels, _) => labels
        case _ => Set.empty
      }
      val withTrueLabels = trueLabels.foldLeft(relevantSelected: RelationalOperator[T]) {
        case (currentOp, label) => operators.AddInto(currentOp, TrueLit, HasLabel(entity, Label(label))(CTBoolean))
      }

      // Fill in missing false label columns
      val falseLabels = entity.cypherType match {
        case CTNode(labels, _) => labels -- trueLabels
        case _ => Set.empty
      }
      val withFalseLabels = falseLabels.foldLeft(withTrueLabels: RelationalOperator[T]) {
        case (currentOp, label) => operators.AddInto(currentOp, FalseLit, HasLabel(entity, Label(label))(CTBoolean))
      }

      // Fill in missing true relType columns
      val trueRelTypes = entityVar.cypherType match {
        case CTRelationship(relTypes, _) => relTypes
        case _ => Set.empty
      }
      val withTrueRelTypes = trueRelTypes.foldLeft(withFalseLabels: RelationalOperator[T]) {
        case (currentOp, relType) => operators.AddInto(currentOp, TrueLit, HasType(entity, RelType(relType))(CTBoolean))
      }

      // Fill in missing false relType columns
      val falseRelTypes = entity.cypherType match {
        case CTRelationship(relTypes, _) => relTypes -- trueRelTypes
        case _ => Set.empty
      }
      val withFalseRelTypes = falseRelTypes.foldLeft(withTrueRelTypes: RelationalOperator[T]) {
        case (currentOp, relType) => operators.AddInto(currentOp, FalseLit, HasType(entity, RelType(relType))(CTBoolean))
      }

      // Fill in missing properties
      val missingProperties = targetHeader.propertiesFor(entity) -- withFalseRelTypes.header.propertiesFor(entity)
      val withProperties = missingProperties.foldLeft(withFalseLabels: RelationalOperator[T]) {
        case (currentOp, propertyExpr) => operators.AddInto(currentOp, NullLit(propertyExpr.cypherType), propertyExpr)
      }

      import Expr._
      assert(targetHeader.expressions == withProperties.header.expressions,
      s"Expected header expressions:\n\t${targetHeader.expressions.toSeq.sorted.mkString(", ")},\ngot\n\t${withProperties.header.expressions.toSeq.sorted.mkString(", ")}")
      withProperties
    }

  }

}

final case class ExtractEntities[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  override val header: RecordHeader,
  extractionVars: Set[Var]
) extends RelationalOperator[T] {

  val varInInputHeader: Var = in.header.entityVars.head

  def selectExpressionsForVar(extractionVar: Var): SelectExpressions = {

    val entityLabels = varInInputHeader.cypherType match {
      case CTNode(labels, _) => labels
      case CTRelationship(types, _) => types
      case _ => throw IllegalArgumentException("CTNode or CTRelationship", extractionVar.cypherType)
    }

    val partiallyAlignedExtractionHeader = in.header
      .withAlias(varInInputHeader as extractionVar)
      .select(extractionVar)

    val missingExpressions = (header.expressions -- partiallyAlignedExtractionHeader.expressions).toSeq
    val overlapExpressions = (header.expressions -- missingExpressions).toSeq

    // Map existing expression in input table to corresponding target header column
    val selectExistingColumns = overlapExpressions.map(e => e.withOwner(extractionVar) -> header.column(e))

    // Map literal default values for missing expressions to corresponding target header column
    val selectMissingColumns: Seq[(Expr, String)] = missingExpressions.map { expr =>
      val columnName = header.column(expr)
      val selectExpr: Expr = expr match {
        case HasLabel(_, label) =>
          if (entityLabels.contains(label.name)) {
            TrueLit
          } else {
            FalseLit
          }
        case HasType(_, relType) =>
          if (entityLabels.contains(relType.name)) {
            TrueLit
          } else {
            FalseLit
          }
        case _ =>
          if (!expr.cypherType.isNullable) {
            throw UnsupportedOperationException(
              s"Cannot align scan on $varInInputHeader by adding a NULL column, because the type for '$expr' is non-nullable"
            )
          }
          NullLit(expr.cypherType)
      }

      selectExpr -> columnName
    }

    (selectExistingColumns ++ selectMissingColumns).sortBy(_._1)
  }

  override def _table: T = {
    val groups = extractionVars.toSeq.map(selectExpressionsForVar)
    in.table.extractEntities(groups)(header, context.parameters)
  }
}

final case class Distinct[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  fields: Set[Var]
) extends RelationalOperator[T] {

  override lazy val _table: T = in.table.distinct(fields.flatMap(header.expressionsFor).map(header.column).toSeq: _*)

}

final case class SimpleDistinct[T <: FlatRelationalTable[T]](in: RelationalOperator[T]) extends RelationalOperator[T] {

  override lazy val _table: T = in.table.distinct
}

final case class Aggregate[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  group: Set[Var],
  aggregations: Set[(Var, Aggregator)]
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = in.header.select(group).withExprs(aggregations.map(_._1))

  override lazy val _table: T = {
    val preparedAggregations = aggregations.map { case (v, agg) => agg -> (header.column(v) -> v.cypherType) }
    in.table.group(group, preparedAggregations)(in.header, context.parameters)
  }
}

final case class OrderBy[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  sortItems: Seq[SortItem[Expr]]
) extends RelationalOperator[T] {

  override lazy val _table: T = {
    val tableSortItems: Seq[(String, Order)] = sortItems.map {
      case Asc(expr) => header.column(expr) -> Ascending
      case Desc(expr) => header.column(expr) -> Descending
    }
    in.table.orderBy(tableSortItems: _*)
  }
}

final case class Skip[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  expr: Expr
) extends RelationalOperator[T] {

  override lazy val _table: T = {
    val skip: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(l) => l
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal or parameter", other)
    }
    in.table.skip(skip)
  }
}

final case class Limit[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  expr: Expr
) extends RelationalOperator[T] {

  override lazy val _table: T = {
    val limit: Long = expr match {
      case IntegerLit(v) => v
      case Param(name) =>
        context.parameters(name) match {
          case CypherInteger(v) => v
          case other => throw IllegalArgumentException("a CypherInteger", other)
        }
      case other => throw IllegalArgumentException("an integer literal", other)
    }
    in.table.limit(limit)
  }
}

final case class EmptyRecords[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  fields: Set[Var] = Set.empty
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = RecordHeader.from(fields)

  override lazy val _table: T = in.table.empty(header)
}

final case class FromGraph[T <: FlatRelationalTable[T]](
  in: RelationalOperator[T],
  logicalGraph: LogicalCatalogGraph
) extends RelationalOperator[T] {

  override def graph: RelationalCypherGraph[T] = resolve(logicalGraph.qualifiedGraphName)

  override def graphName: QualifiedGraphName = logicalGraph.qualifiedGraphName

}

// Binary

final case class Join[T <: FlatRelationalTable[T]](
  lhs: RelationalOperator[T],
  rhs: RelationalOperator[T],
  joinExprs: Seq[(Expr, Expr)] = Seq.empty,
  joinType: JoinType
) extends RelationalOperator[T] {

  override lazy val header: RecordHeader = lhs.header join rhs.header

  override lazy val _table: T = {
    val joinCols = joinExprs.map { case (l, r) => header.column(l) -> rhs.header.column(r) }
    lhs.table.join(rhs.table, joinType, joinCols: _*)
  }
}

/**
  * Computes the union of the two input operators. The two inputs must have identical headers.
  * This operation does not remove duplicates.
  *
  * The output header of this operation is identical to the input headers.
  *
  * @param lhs the first operand
  * @param rhs the second operand
  */
// TODO: rename to UnionByName
// TODO: refactor to n-ary operator (i.e. take List[PhysicalOperator] as input)
final case class TabularUnionAll[T <: FlatRelationalTable[T]](
  lhs: RelationalOperator[T],
  rhs: RelationalOperator[T]
) extends RelationalOperator[T] {

  override lazy val _table: T = {
    val lhsTable = lhs.table
    val rhsTable = rhs.table

    val leftColumns = lhsTable.physicalColumns
    val rightColumns = rhsTable.physicalColumns

    if (leftColumns.size != rightColumns.size) {
      throw IllegalArgumentException("same number of columns", s"left: $leftColumns right: $rightColumns")
    }
    if (leftColumns.toSet != rightColumns.toSet) {
      throw IllegalArgumentException("same column names", s"left: $leftColumns right: $rightColumns")
    }

    val orderedRhsTable = if (leftColumns != rightColumns) {
      rhsTable.select(leftColumns: _*)
    } else {
      rhsTable
    }

    lhsTable.unionAll(orderedRhsTable)
  }
}

final case class ConstructGraph[T <: FlatRelationalTable[T]](
  lhs: RelationalOperator[T],
  rhs: RelationalOperator[T],
  construct: LogicalPatternGraph
) extends RelationalOperator[T] {

}

// N-ary

final case class GraphUnionAll[T <: FlatRelationalTable[T]](
  inputs: List[RelationalOperator[T]],
  qgn: QualifiedGraphName
) extends RelationalOperator[T] {
  //  require(inputs.nonEmpty, "GraphUnionAll requires at least one input")
  //
  //  override lazy val tagStrategy = {
  //    computeRetaggings(inputs.map(r => r.graphName -> r.graph.tags).toMap)
  //  }
  //
  //  override lazy val graphName = qgn
  //
  //  override lazy val graph = {
  //    val graphWithTagStrategy = inputs.map(i => i.graph -> tagStrategy(i.graphName)).toMap
  //    CAPSUnionGraph(graphWithTagStrategy)
  //  }
  //
  //}
}
