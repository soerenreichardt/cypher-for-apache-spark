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
package org.opencypher.okapi.api.value

import java.sql.Date
import java.util.Objects

import org.opencypher.okapi.api.value.CypherValue.CypherEntity._
import org.opencypher.okapi.api.value.CypherValue.CypherNode._
import org.opencypher.okapi.api.value.CypherValue.CypherRelationship._
import org.opencypher.okapi.impl.exception.{IllegalArgumentException, UnsupportedOperationException}
import upickle.Js

import scala.reflect.{ClassTag, classTag}
import scala.util.Try
import scala.util.hashing.MurmurHash3

object CypherValue {

  /**
    * Converts a Scala/Java value to a compatible Cypher value, fails if the conversion is not supported.
    *
    * @param v value to convert
    * @return compatible CypherValue
    */
  def apply(v: Any): CypherValue = {
    def seqToCypherList(s: Seq[_]): CypherList = s.map(CypherValue(_)).toList

    v match {
      case cv: CypherValue => cv
      case null => CypherNull
      case jb: java.lang.Byte => jb.toLong
      case js: java.lang.Short => js.toLong
      case ji: java.lang.Integer => ji.toLong
      case jl: java.lang.Long => jl.toLong
      case jf: java.lang.Float => jf.toDouble
      case jd: java.lang.Double => jd.toDouble
      case js: java.lang.String => js.toString
      case jb: java.lang.Boolean => jb.booleanValue
      case jl: java.util.List[_] => seqToCypherList(jl.toArray)
      case dt: java.sql.Date => dt
      case a: Array[_] => seqToCypherList(a)
      case s: Seq[_] => seqToCypherList(s)
      case m: Map[_, _] => m.map { case (k, cv) => k.toString -> CypherValue(cv) }
      case b: Byte => b.toLong
      case s: Short => s.toLong
      case i: Int => i.toLong
      case l: Long => l
      case f: Float => f.toDouble
      case d: Double => d
      case b: Boolean => b
      case invalid =>
        throw IllegalArgumentException(
          "a value that can be converted to a Cypher value", s"$invalid of type ${invalid.getClass.getName}")
    }
  }

  /**
    * Converts a Scala/Java value to a compatible Cypher value.
    *
    * @param v value to convert
    * @return Some compatible CypherValue or None
    */
  def get(v: Any): Option[CypherValue] = {
    Try(apply(v)).toOption
  }

  /**
    * Attempts to extract the wrapped value from a CypherValue.
    *
    * @param cv CypherValue to extract from
    * @return none or some extracted value.
    */
  def unapply(cv: CypherValue): Option[Any] = {
    Option(cv).flatMap(v => Option(v.value))
  }

  /**
    * CypherValue is a wrapper for Scala/Java classes that represent valid Cypher values.
    */
  sealed trait CypherValue extends Any {
    /**
      * @return wrapped value
      */
    def value: Any

    /**
      * @return null-safe version of [[value]]
      */
    def getValue: Option[Any]

    /**
      * @return unwraps the Cypher value into Scala/Java structures. Unlike [[value]] this is done recursively for the
      *         Cypher values stored inside of maps and lists.
      */
    def unwrap: Any

    /**
      * @return true iff the stored value is null.
      */
    def isNull: Boolean = Objects.isNull(value)

    /**
      * Safe version of [[cast]]
      */
    def as[V: ClassTag]: Option[V] = {
      this match {
        case cv: V => Some(cv)
        case _ =>
          value match {
            case v: V => Some(v)
            case _ => None
          }
      }
    }

    /**
      * Attempts to cast the Cypher value to `V`, fails when this is not supported.
      */
    def cast[V: ClassTag]: V = as[V].getOrElse(throw UnsupportedOperationException(
      s"Cannot cast $value of type ${value.getClass.getSimpleName} to ${classTag[V].runtimeClass.getSimpleName}"))


    /**
      * String of the Scala representation of this value.
      */
    override def toString: String = Objects.toString(unwrap)

    /**
      * Hash code of the Scala representation.
      */
    override def hashCode: Int = Objects.hashCode(unwrap)

    /**
      * Structural comparison of the Scala representation.
      *
      * This is NOT Cypher equality or equivalence.
      */
    override def equals(other: Any): Boolean = {
      other match {
        case cv: CypherValue => Objects.equals(unwrap, cv.unwrap)
        case _ => false
      }
    }

    /**
      * A Cypher string representation. For more information about the exact format of these, please refer to
      * [[https://github.com/opencypher/openCypher/tree/master/tck#format-of-the-expected-results the openCypher TCK]].
      */
    def toCypherString: String = {
      this match {
        case CypherString(s) => s"'${escape(s)}'"
        case CypherList(l) => l.map(_.toCypherString).mkString("[", ", ", "]")
        case CypherMap(m) =>
          m.toSeq
            .sortBy(_._1)
            .map { case (k, v) => s"`${escape(k)}`: ${v.toCypherString}" }
            .mkString("{", ", ", "}")
        case CypherRelationship(_, _, _, relType, props) =>
          s"[:`${escape(relType)}`${
            if (props.isEmpty) ""
            else s" ${props.toCypherString}"
          }]"
        case CypherNode(_, labels, props) =>
          val labelString =
            if (labels.isEmpty) ""
            else labels.toSeq.sorted.map(escape).mkString(":`", "`:`", "`")
          val propertyString = if (props.isEmpty) ""
          else s"${props.toCypherString}"
          Seq(labelString, propertyString)
            .filter(_.nonEmpty)
            .mkString("(", " ", ")")
        case _ => Objects.toString(value)
      }
    }

    def toJson: Js.Value = {
      this match {
        case CypherNull => Js.Null
        case CypherString(s) => Js.Str(s)
        case CypherList(l) => l.map(_.toJson)
        case CypherMap(m) => m.mapValues(_.toJson).toSeq.sortBy(_._1)
        case CypherRelationship(id, startId, endId, relType, properties) =>
          Js.Obj(
            idJsonKey -> Js.Str(id.toString),
            typeJsonKey -> Js.Str(relType),
            startIdJsonKey -> Js.Str(startId.toString),
            endIdJsonKey -> Js.Str(endId.toString),
            propertiesJsonKey -> properties.toJson
          )
        case CypherNode(id, labels, properties) =>
          Js.Obj(
            idJsonKey -> Js.Str(id.toString),
            labelsJsonKey -> labels.toSeq.sorted.map(Js.Str),
            propertiesJsonKey -> properties.toJson
          )
        case CypherFloat(d) => Js.Num(d)
        case CypherInteger(l) => Js.Str(l.toString) // `Js.Num` would lose precision
        case CypherBoolean(b) => Js.Bool(b)
        case other => Js.Str(other.value.toString)
      }
    }

    private def escape(str: String): String = {
      str
        .replaceAllLiterally("""\""", """\\""")
        .replaceAllLiterally("'", "\\'")
        .replaceAllLiterally("\"", "\\\"")
    }

    private[opencypher] def isOrContainsNull: Boolean = isNull || {
      this match {
        case l: CypherList => l.value.exists(_.isOrContainsNull)
        case m: CypherMap => m.value.valuesIterator.exists(_.isOrContainsNull)
        case _ => false
      }
    }

  }

  object CypherNull extends CypherValue {
    override def value: Null = null

    override def unwrap: Null = value

    override def getValue: Option[Any] = None
  }

  implicit class CypherString(val value: String) extends AnyVal with PrimitiveCypherValue[String]

  implicit class CypherBoolean(val value: Boolean) extends AnyVal with PrimitiveCypherValue[Boolean]

  sealed trait CypherNumber[+V] extends Any with PrimitiveCypherValue[V]

  implicit class CypherInteger(val value: Long) extends AnyVal with CypherNumber[Long]

  implicit class CypherFloat(val value: Double) extends AnyVal with CypherNumber[Double]

  implicit class CypherDateTime(val value: java.sql.Date) extends AnyVal with MaterialCypherValue[java.sql.Date] {
    override def unwrap: Any = value
  }

  implicit class CypherMap(val value: Map[String, CypherValue]) extends AnyVal with MaterialCypherValue[Map[String, CypherValue]] {
    override def unwrap: Map[String, Any] = value.map { case (k, v) => k -> v.unwrap }

    def isEmpty: Boolean = value.isEmpty

    def keys: Set[String] = value.keySet

    def get(k: String): Option[CypherValue] = value.get(k)

    def getOrElse(k: String, default: CypherValue = CypherNull): CypherValue = value.getOrElse(k, default)

    def apply(k: String): CypherValue = value.getOrElse(k, CypherNull)

    def ++(other: CypherMap): CypherMap = value ++ other.value

    def updated(k: String, v: CypherValue): CypherMap = value.updated(k, v)
  }

  object CypherMap extends UnapplyValue[Map[String, CypherValue], CypherMap] {
    def apply(values: (String, Any)*): CypherMap = {
      values.map { case (k, v) => k -> CypherValue(v) }.toMap
    }

    val empty: CypherMap = Map.empty[String, CypherValue]

  }

  implicit class CypherList(val value: List[CypherValue]) extends AnyVal with MaterialCypherValue[List[CypherValue]] {
    override def unwrap: List[Any] = value.map(_.unwrap)
  }

  object CypherList extends UnapplyValue[List[CypherValue], CypherList] {
    def apply(elem: Any*): CypherList = elem.map(CypherValue(_)).toList

    val empty: CypherList = List.empty[CypherValue]
  }

  trait CypherEntity[Id] extends Product with MaterialCypherValue[CypherEntity[Id]] {
    type I <: CypherEntity[Id]

    def id: Id

    def properties: CypherMap

    override def hashCode: Int = {
      MurmurHash3.orderedHash(productIterator, MurmurHash3.stringHash(productPrefix))
    }

    override def equals(other: Any): Boolean = other match {
      case that: CypherEntity[_] =>
        (that canEqual this) && haveEqualValues(this.productIterator, that.productIterator)
      case _ =>
        false
    }

    protected def haveEqualValues(a: Iterator[Any], b: Iterator[Any]): Boolean = {
      while (a.hasNext && b.hasNext) {
        if (a.next != b.next) return false
      }
      a.hasNext == b.hasNext
    }

    override def productPrefix: String = getClass.getSimpleName

    override def toString = s"$productPrefix(${productIterator.mkString(", ")})"

    def withProperty(key: String, value: CypherValue): I

  }

  object CypherEntity {

    val idJsonKey: String = "id"
    val propertiesJsonKey: String = "properties"

  }

  trait CypherNode[Id] extends CypherEntity[Id] with MaterialCypherValue[CypherNode[Id]] {
    override type I <: CypherNode[Id]

    def id: Id

    def labels: Set[String]

    override def value: CypherNode[Id] = this

    override def unwrap: CypherNode[Id] = this

    override def productArity: Int = 3

    override def productElement(n: Int): Any = n match {
      case 0 => id
      case 1 => labels
      case 2 => properties
      case other => throw IllegalArgumentException("a valid product index", s"$other")
    }

    override def canEqual(that: Any): Boolean = that.isInstanceOf[CypherNode[_]]

    def copy(id: Id = id, labels: Set[String] = labels, properties: CypherMap = properties): I

    def withLabel(label: String): I = {
      copy(labels = labels + label)
    }

    override def withProperty(key: String, value: CypherValue): I = {
      copy(properties = properties.value.updated(key, value))
    }

  }

  object CypherNode {

    val labelsJsonKey: String = "labels"

    def unapply[Id](n: CypherNode[Id]): Option[(Id, Set[String], CypherMap)] = {
      Option(n).map(node => (node.id, node.labels, node.properties))
    }

  }

  trait CypherRelationship[Id] extends CypherEntity[Id] with MaterialCypherValue[CypherRelationship[Id]] with Product {

    override type I <: CypherRelationship[Id]

    def id: Id

    def startId: Id

    def endId: Id

    def relType: String

    override def value: CypherRelationship[Id] = this

    override def unwrap: CypherRelationship[Id] = this

    override def productArity: Int = 5

    override def productElement(n: Int): Any = n match {
      case 0 => id
      case 1 => startId
      case 2 => endId
      case 3 => relType
      case 4 => properties
      case other => throw IllegalArgumentException("a valid product index", s"$other")
    }

    override def canEqual(that: Any): Boolean = that.isInstanceOf[CypherRelationship[_]]

    def copy(
      id: Id = id,
      source: Id = startId,
      target: Id = endId,
      relType: String = relType,
      properties: CypherMap = properties): I

    def withType(relType: String): I = {
      copy(relType = relType)
    }

    override def withProperty(key: String, value: CypherValue): I = {
      copy(properties = properties.value.updated(key, value))
    }

  }

  object CypherRelationship {

    val typeJsonKey: String = "type"
    val startIdJsonKey: String = "startId"
    val endIdJsonKey: String = "endId"

    def unapply[Id](r: CypherRelationship[Id]): Option[(Id, Id, Id, String, CypherMap)] = {
      Option(r).map(rel => (rel.id, rel.startId, rel.endId, rel.relType, rel.properties))
    }

  }

  trait MaterialCypherValue[+T] extends Any with CypherValue {
    override def value: T

    override def getValue: Option[T] = Option(value)
  }

  /**
    * A primitive Cypher value is one that does not contain any other Cypher values.
    */
  trait PrimitiveCypherValue[+T] extends Any with MaterialCypherValue[T] {
    override def unwrap: T = value
  }

  abstract class UnapplyValue[V, CV <: MaterialCypherValue[V]] {
    def unapply(v: CV): Option[V] = Option(v).flatMap(_.getValue)
  }

  object CypherString extends UnapplyValue[String, CypherString]

  object CypherBoolean extends UnapplyValue[Boolean, CypherBoolean]

  object CypherInteger extends UnapplyValue[Long, CypherInteger]

  object CypherFloat extends UnapplyValue[Double, CypherFloat]

}
