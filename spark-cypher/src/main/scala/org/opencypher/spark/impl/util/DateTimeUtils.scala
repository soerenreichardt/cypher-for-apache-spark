package org.opencypher.spark.impl.util

import org.apache.spark.sql.DataFrame
import org.opencypher.okapi.api.value.CypherValue.{CypherFloat, CypherInteger, CypherMap, CypherString}
import org.opencypher.okapi.impl.exception.IllegalArgumentException
import org.opencypher.okapi.ir.api.expr.{MapExpression, Param}
import org.opencypher.okapi.relational.impl.table.RecordHeader

object DateTimeUtils {

  def fromMapExpr(map: MapExpression)(implicit header: RecordHeader, df: DataFrame, parameters: CypherMap): String = {
    val convertedMapElements = map.items.map(m => m._1 -> (m._2 match {
      case Param(name) => parameters(name) match {
        case CypherFloat(d) => d.toString
        case CypherInteger(l) => l.toString
        case CypherString(s) => s
        case other => IllegalArgumentException("a parameter of type CypherFloat, CypherInteger or CypherString", other)
      }
      case other => throw IllegalArgumentException("a param", other)
    }))
    val withValidStringLengths = convertedMapElements.map {
      case ("year", value) if value.
    }
  }

}
