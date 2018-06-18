package org.opencypher.flink.impl

import org.opencypher.flink.impl.util.TagSupport._
import org.opencypher.flink.schema.CAPFSchema
import org.opencypher.flink.schema.CAPFSchema._
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTNode, CTRelationship}
import org.opencypher.okapi.ir.api.expr.Var
import org.opencypher.okapi.relational.api.schema.RelationalSchema._

object CAPFUnionGraph {
  def apply(graphs: CAPFGraph*)(implicit session: CAPFSession): CAPFUnionGraph = {
    CAPFUnionGraph(computeRetaggings(graphs.map(g => g -> g.tags).toMap))
  }
}

final case class CAPFUnionGraph(graphs: Map[CAPFGraph, Map[Int, Int]])
  (implicit val session: CAPFSession) extends CAPFGraph {

  require(graphs.nonEmpty, "Union requires at least one graph")

  override lazy val tags: Set[Int] = graphs.values.flatMap(_.values).toSet

  override def toString = s"CAPFUnionGraph(graphs=[${graphs.mkString(",")}])"

  override lazy val schema: CAPFSchema = {
    graphs.keys.map(g => g.schema).foldLeft(Schema.empty)(_ ++ _).asCapf
  }

  private def map(f: CAPFGraph => CAPFGraph): CAPFUnionGraph =
    CAPFUnionGraph(graphs.keys.map(f).zip(graphs.keys).toMap.mapValues(graphs))

  override def nodes(name: String, nodeCypherType: CTNode): CAPFRecords = {
    val node = Var(name)(nodeCypherType)
    val targetHeader = schema.headerForNode(node)
    val nodeScans = graphs.keys
      .filter(nodeCypherType.labels.isEmpty || _.schema.labels.intersect(nodeCypherType.labels).nonEmpty)
      .map {
        graph =>
          val nodeScan = graph.nodes(name, nodeCypherType)
          nodeScan.retag(graphs(graph))
      }

    alignRecords(nodeScans.toSeq, node, targetHeader)
      .map(_.distinct)
      .getOrElse(CAPFRecords.empty(targetHeader))
  }

  override def relationships(name: String, relCypherType: CTRelationship): CAPFRecords = {
    val rel = Var(name)(relCypherType)
    val targetHeader = schema.headerForRelationship(rel)
    val relScans = graphs.keys
      .filter(relCypherType.types.isEmpty || _.schema.relationshipTypes.intersect(relCypherType.types).nonEmpty)
      .map { graph =>
        val relScan = graph.relationships(name, relCypherType)
        relScan.retag(graphs(graph))
      }

    alignRecords(relScans.toSeq, rel, targetHeader)
      .map(_.distinct)
      .getOrElse(CAPFRecords.empty(targetHeader))
  }

}