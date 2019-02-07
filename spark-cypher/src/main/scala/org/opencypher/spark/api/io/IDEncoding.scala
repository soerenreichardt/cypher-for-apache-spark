/*
 * Copyright (c) 2016-2019 "Neo4j Sweden, AB" [https://neo4j.com]
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
package org.opencypher.spark.api.io

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, functions}
import org.opencypher.okapi.ir.api.expr.PrefixId.GraphIdPrefix

//class LongToByteArray extends UnaryExpression with ExpectsInputTypes with NullIntolerant {
//
//}

object IDEncoding {

  type CAPSId = Array[Byte]

  private final val moreBytesBitMask: Long = Integer.parseInt("10000000", 2)
  private final val varLength7BitMask: Long = Integer.parseInt("01111111", 2)
  private final val otherBitsMask = ~varLength7BitMask
  private final val maxBytesForLongVarEncoding = 9

  // Same encoding as as Base 128 Varints @ https://developers.google.com/protocol-buffers/docs/encoding
  @inline
  private final def encodeLong(l: Long): Array[Byte] = {
    assert(l >= 0, "var-length encoding only supports positive values")

    val tempResult = new Array[Byte](maxBytesForLongVarEncoding)

    var remainder = l
    var index = 0

    while ((remainder & otherBitsMask) != 0) {
      tempResult(index) = ((remainder & varLength7BitMask) | moreBytesBitMask).toByte
      remainder >>>= 7
      index += 1
    }
    tempResult(index) = remainder.toByte

    val result = new Array[Byte](index + 1)
    System.arraycopy(tempResult, 0, result, 0, index + 1)
    result
  }

  // Same encoding as as Base 128 Varints @ https://developers.google.com/protocol-buffers/docs/encoding
  @inline
  private final def decodeLong(input: Array[Byte]): Long = {
    assert(input.nonEmpty)

    var index = 0
    var currentByte = input(index)
    var decoded = currentByte & varLength7BitMask
    var nextLeftShift = 7

    while ((currentByte & moreBytesBitMask) != 0) {
      index += 1
      currentByte = input(index)
      decoded |= (currentByte & varLength7BitMask) << nextLeftShift
      nextLeftShift += 7
    }
    assert(index == input.length - 1)
    decoded
  }

  private val encodeUdf = functions.udf(encodeLong _)

  private def addPrefix(a: CAPSId, p: Byte): CAPSId = {
    val n = new Array[Byte](a.length + 1)
    n(0) = p
    System.arraycopy(a, 0, n, 1, a.length)
    n
  }

  private val l56 = lit(56).expr
  private val l48 = lit(48).expr
  private val l40 = lit(40).expr
  private val l32 = lit(32).expr
  private val l24 = lit(24).expr
  private val l16 = lit(16).expr
  private val l8 = lit(8).expr

  implicit class ColumnIdEncoding(val c: Column) extends AnyVal {

    def encodeLongAsCAPSId(name: String): Column = encodeLongAsCAPSId.as(name)

    def encodeLongAsCAPSId: Column = encodeUdf(c)
  }

  implicit class LongIdEncoding(val l: Long) extends AnyVal {

    def encodeAsCAPSId: CAPSId = encodeLong(l)

    def withPrefix(prefix: Int): CAPSId = l.encodeAsCAPSId.withPrefix(prefix.toByte)

  }

  implicit class LongIdDecoding(val a: Array[Byte]) extends AnyVal {

    def decodeToLong: Long = decodeLong(a)

  }

  implicit class RichCAPSId(val id: CAPSId) extends AnyVal {

    def withPrefix(prefix: GraphIdPrefix): CAPSId = addPrefix(id, prefix)

  }

}
