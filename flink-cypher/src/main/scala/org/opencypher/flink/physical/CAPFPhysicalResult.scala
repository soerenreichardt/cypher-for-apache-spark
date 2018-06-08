package org.opencypher.flink.physical

import org.opencypher.flink.{CAPFGraph, CAPFRecords, CAPFSession}
import org.opencypher.okapi.api.graph.QualifiedGraphName
import org.opencypher.okapi.relational.api.physical.PhysicalResult

case class CAPFPhysicalResult(
  records: CAPFRecords,
  workingGraph: CAPFGraph,
  workingGraphName: QualifiedGraphName,
  tagStrategy: Map[QualifiedGraphName, Map[Int, Int]] = Map.empty
)
  extends PhysicalResult[CAPFRecords, CAPFGraph] {

  override def mapRecordsWithDetails(f: CAPFRecords => CAPFRecords): CAPFPhysicalResult =
    copy(records = f(records))
}