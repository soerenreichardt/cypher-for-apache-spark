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
package org.opencypher.okapi.ir.impl.typer

import cats.data.NonEmptyList
import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types._
import org.opencypher.okapi.api.value.CypherValue
import org.opencypher.okapi.ir.test.support.Neo4jAstTestSupport
import org.opencypher.okapi.testing.BaseTestSuite
import org.opencypher.v9_0.expressions._
import org.opencypher.v9_0.expressions.functions.Tail
import org.opencypher.v9_0.util.symbols
import org.scalatest.Assertion
import org.scalatest.mockito.MockitoSugar

import scala.language.reflectiveCalls

class SchemaTyperTest extends BaseTestSuite with Neo4jAstTestSupport with MockitoSugar {

  val schema: Schema = Schema.empty
    .withNodePropertyKeys("Person")("name" -> CTString, "age" -> CTInteger)
    .withNodePropertyKeys("Foo")("name" -> CTBoolean, "age" -> CTFloat)
    .withRelationshipPropertyKeys("KNOWS")("since" -> CTInteger, "relative" -> CTBoolean)

  val typer = SchemaTyper(schema)

  it("should report good error on unsupported functions") {
    implicit val context: TypeTracker = typeTracker("a" -> CTList(CTInteger))

    val f = Tail.asInvocation(Variable("a")(pos))(pos)

    assertExpr.from("tail(a)") shouldFailToInferTypeWithErrors(
      UnsupportedExpr(f), NoSuitableSignatureForExpr(f, Seq(CTList(CTInteger)))
    )
  }

  it("should type DateTime from string") {
    implicit val context: TypeTracker = typeTracker("d" -> CTDateTime)

    assertExpr.from("datetime('2010-12-10')") shouldHaveInferredType CTDateTime.nullable
    assertExpr.from("datetime('2010-12-10T00:00:00.000')") shouldHaveInferredType CTDateTime.nullable
  }

  it("should type DateTime from map") {
    implicit val context: TypeTracker = typeTracker("d" -> CTDateTime)

    assertExpr.from("datetime({year: 2015, month: 12, day: 8})") shouldHaveInferredType CTDateTime.nullable
  }

  it("should type trim(), ltrim(), rtrim()") {
    implicit val context: TypeTracker = typeTracker("n" -> CTString)

    assertExpr.from("trim(n)") shouldHaveInferredType CTString.nullable
    assertExpr.from("ltrim(n)") shouldHaveInferredType CTString.nullable
    assertExpr.from("rtrim(n)") shouldHaveInferredType CTString.nullable
  }

  it("should type timestamp()") {
    implicit val context: TypeTracker = typeTracker()

    assertExpr.from("timestamp()") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("timestamp() + 5") shouldHaveInferredType CTInteger.nullable
  }

  it("should type CASE") {
    implicit val context: TypeTracker = typeTracker("a" -> CTInteger, "b" -> CTInteger.nullable, "c" -> CTString)

    assertExpr.from("CASE WHEN a > b THEN c END") shouldHaveInferredType CTString
    assertExpr.from("CASE WHEN c THEN a WHEN c THEN b END") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("CASE WHEN c THEN a WHEN a THEN b ELSE c END") shouldHaveInferredType CTAny.nullable
  }

  it("should type coalesce()") {
    implicit val context: TypeTracker = typeTracker("a" -> CTInteger, "b" -> CTInteger.nullable, "c" -> CTString)

    assertExpr.from("coalesce(a, a)") shouldHaveInferredType CTInteger
    assertExpr.from("coalesce(b, b)") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("coalesce(a, b)") shouldHaveInferredType CTInteger
    assertExpr.from("coalesce(a, c)") shouldHaveInferredType CTInteger
    assertExpr.from("coalesce(b, c)") shouldHaveInferredType CTAny.nullable
    assertExpr.from("coalesce()") shouldFailToInferTypeWithErrors
      WrongNumberOfArguments("coalesce()", 1, 0)
  }

  it("typing exists()") {
    implicit val context: TypeTracker = typeTracker("n" -> CTNode)

    assertExpr.from("exists(n.prop)") shouldHaveInferredType CTBoolean
    assertExpr.from("exists([n.prop])") shouldFailToInferTypeWithErrors
      InvalidArgument("exists([n.prop])", "[n.prop]")
    assertExpr.from("exists()") shouldFailToInferTypeWithErrors
      WrongNumberOfArguments("exists()", 1, 0)
    assertExpr.from("exists(n.prop, n.prop)") shouldFailToInferTypeWithErrors
      WrongNumberOfArguments("exists(n.prop, n.prop)", 1, 2)
  }

  it("typing count()") {
    implicit val context: TypeTracker = typeTracker("a" -> CTNode)

    assertExpr.from("count(*)") shouldHaveInferredType CTInteger
    assertExpr.from("count(a)") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("count(a.name)") shouldHaveInferredType CTInteger.nullable
  }

  it("typing toString()") {
    implicit val context: TypeTracker = typeTracker("a" -> CTInteger, "b" -> CTBoolean, "c" -> CTFloat, "d" -> CTString)

    assertExpr.from("toString(a)") shouldHaveInferredType CTString
    assertExpr.from("toString(b)") shouldHaveInferredType CTString
    assertExpr.from("toString(c)") shouldHaveInferredType CTString
    assertExpr.from("toString(d)") shouldHaveInferredType CTString
  }

  it("typing toBoolean()") {
    implicit val context: TypeTracker = typeTracker("true" -> CTBoolean, "false" -> CTBoolean)

    assertExpr.from("toBoolean(true)") shouldHaveInferredType CTBoolean
    assertExpr.from("toBoolean(false)") shouldHaveInferredType CTBoolean
  }

  describe("logarithmic functions") {
    it("can type sqrt()") {
      implicit val context: TypeTracker = typeTracker("a" -> CTFloat, "b" -> CTInteger, "c" -> CTNull)

      assertExpr.from("sqrt(a)") shouldHaveInferredType CTFloat
      assertExpr.from("sqrt(b)") shouldHaveInferredType CTFloat
      assertExpr.from("sqrt(c)") shouldHaveInferredType CTNull
    }

    it("can type log()") {
      implicit val context: TypeTracker = typeTracker("a" -> CTFloat, "b" -> CTInteger, "c" -> CTNull)

      assertExpr.from("log(a)") shouldHaveInferredType CTFloat
      assertExpr.from("log(b)") shouldHaveInferredType CTFloat
      assertExpr.from("log(c)") shouldHaveInferredType CTNull
    }

    it("can type log10()") {
      implicit val context: TypeTracker = typeTracker("a" -> CTFloat, "b" -> CTInteger, "c" -> CTNull)

      assertExpr.from("log10(a)") shouldHaveInferredType CTFloat
      assertExpr.from("log10(b)") shouldHaveInferredType CTFloat
      assertExpr.from("log10(c)") shouldHaveInferredType CTNull
    }

    it("can type exp()") {
      implicit val context: TypeTracker = typeTracker("a" -> CTFloat, "b" -> CTInteger, "c" -> CTNull)

      assertExpr.from("exp(a)") shouldHaveInferredType CTFloat
      assertExpr.from("exp(b)") shouldHaveInferredType CTFloat
      assertExpr.from("exp(c)") shouldHaveInferredType CTNull
    }

    it("can type e()") {
      implicit val context: TypeTracker = typeTracker()

      assertExpr.from("e()") shouldHaveInferredType CTFloat.nullable
    }
  }

  describe("numeric functions") {
    it("can type abs()") {
      implicit val context: TypeTracker = typeTracker("a" -> CTFloat, "b" -> CTInteger, "c" -> CTNull)

      assertExpr.from("abs(a)") shouldHaveInferredType CTFloat
      assertExpr.from("abs(b)") shouldHaveInferredType CTInteger
      assertExpr.from("abs(c)") shouldHaveInferredType CTNull
    }

    it("can type ceil()") {
      implicit val context: TypeTracker = typeTracker("a" -> CTFloat, "b" -> CTInteger, "c" -> CTNull)

      assertExpr.from("ceil(a)") shouldHaveInferredType CTFloat
      assertExpr.from("ceil(b)") shouldHaveInferredType CTFloat
      assertExpr.from("ceil(c)") shouldHaveInferredType CTNull
    }

    it("can type floor()") {
      implicit val context: TypeTracker = typeTracker("a" -> CTFloat, "b" -> CTInteger, "c" -> CTNull)

      assertExpr.from("floor(a)") shouldHaveInferredType CTFloat
      assertExpr.from("floor(b)") shouldHaveInferredType CTFloat
      assertExpr.from("floor(c)") shouldHaveInferredType CTNull
    }

    it("can type rand()") {
      implicit val context: TypeTracker = typeTracker()

      assertExpr.from("rand()") shouldHaveInferredType CTFloat.nullable
    }

    it("can type round()") {
      implicit val context: TypeTracker = typeTracker("a" -> CTFloat, "b" -> CTInteger, "c" -> CTNull)

      assertExpr.from("round(a)") shouldHaveInferredType CTFloat
      assertExpr.from("round(b)") shouldHaveInferredType CTFloat
      assertExpr.from("round(c)") shouldHaveInferredType CTNull
    }

    it("can type sign()") {
      implicit val context: TypeTracker = typeTracker("a" -> CTFloat, "b" -> CTInteger, "c" -> CTNull)

      assertExpr.from("sign(a)") shouldHaveInferredType CTInteger
      assertExpr.from("sign(b)") shouldHaveInferredType CTInteger
      assertExpr.from("sign(c)") shouldHaveInferredType CTNull
    }
  }

  it("typing property of node without label") {
    implicit val context: TypeTracker = typeTracker("a" -> CTNode)

    assertExpr.from("a.name") shouldHaveInferredType CTAny
    assertExpr.from("a.age") shouldHaveInferredType CTNumber
  }

  it("typing add") {
    implicit val context: TypeTracker =
      typeTracker("a" -> CTInteger, "b" -> CTFloat, "c" -> CTNumber, "d" -> CTAny.nullable, "e" -> CTBoolean)

    assertExpr.from("a + a") shouldHaveInferredType CTInteger
    assertExpr.from("b + b") shouldHaveInferredType CTFloat
    assertExpr.from("a + b") shouldHaveInferredType CTFloat
    assertExpr.from("b + a") shouldHaveInferredType CTFloat
    assertExpr.from("a + c") shouldHaveInferredType CTNumber
    assertExpr.from("c + b") shouldHaveInferredType CTNumber

    assertExpr.from("a + d") shouldHaveInferredType CTAny.nullable
    assertExpr.from("d + c") shouldHaveInferredType CTAny.nullable

    assertExpr.from("a + e") shouldFailToInferTypeWithErrors
      NoSuitableSignatureForExpr("a + e", Seq(CTInteger, CTBoolean))
    assertExpr.from("e + a") shouldFailToInferTypeWithErrors
      NoSuitableSignatureForExpr("e + a", Seq(CTBoolean, CTInteger))
    assertExpr.from("e + e") shouldFailToInferTypeWithErrors
      NoSuitableSignatureForExpr("e + e", Seq(CTBoolean, CTBoolean))
  }

  it("typing add for string and list concatenation") {
    assertExpr.from("'foo' + 'bar'") shouldHaveInferredType CTString
    assertExpr.from("[] + [1, 2, 3]") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("[true] + [1, 2, 3]") shouldHaveInferredType CTList(CTAny)

    assertExpr.from("'foo' + 1") shouldHaveInferredType CTString
    assertExpr.from("'foo' + 3.14") shouldHaveInferredType CTString
    assertExpr.from("'foo' + ['bar', 'giz']") shouldHaveInferredType CTList(CTString)

    assertExpr.from("[] + 1") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("[3.14] + 1") shouldHaveInferredType CTList(CTNumber)

    assertExpr.from("['foo'] + null") shouldFailToInferTypeWithErrors
      UnsupportedExpr(parseExpr("['foo'] + null"))
  }

  it("typing subtract") {
    implicit val context: TypeTracker =
      typeTracker("a" -> CTInteger, "b" -> CTFloat, "c" -> CTNumber, "d" -> CTAny.nullable, "e" -> CTString)

    assertExpr.from("a - a") shouldHaveInferredType CTInteger
    assertExpr.from("b - b") shouldHaveInferredType CTFloat
    assertExpr.from("a - b") shouldHaveInferredType CTFloat
    assertExpr.from("b - a") shouldHaveInferredType CTFloat
    assertExpr.from("a - c") shouldHaveInferredType CTNumber
    assertExpr.from("c - b") shouldHaveInferredType CTNumber

    assertExpr.from("a - d") shouldHaveInferredType CTAny.nullable
    assertExpr.from("d - c") shouldHaveInferredType CTAny.nullable

    assertExpr.from("a - e") shouldFailToInferTypeWithErrors
      InvalidType("e", Seq(CTInteger, CTFloat, CTNumber), CTString)
  }

  it("typing multiply") {
    implicit val context: TypeTracker =
      typeTracker("a" -> CTInteger, "b" -> CTFloat, "c" -> CTNumber, "d" -> CTAny.nullable, "e" -> CTString)

    assertExpr.from("a * a") shouldHaveInferredType CTInteger
    assertExpr.from("b * b") shouldHaveInferredType CTFloat
    assertExpr.from("a * b") shouldHaveInferredType CTFloat
    assertExpr.from("b * a") shouldHaveInferredType CTFloat
    assertExpr.from("a * c") shouldHaveInferredType CTNumber
    assertExpr.from("c * b") shouldHaveInferredType CTNumber

    assertExpr.from("a * d") shouldHaveInferredType CTAny.nullable
    assertExpr.from("d * c") shouldHaveInferredType CTAny.nullable

    assertExpr.from("a * e") shouldFailToInferTypeWithErrors
      InvalidType("e", Seq(CTInteger, CTFloat, CTNumber), CTString)
  }

  it("typing divide") {
    implicit val context: TypeTracker =
      typeTracker("a" -> CTInteger, "b" -> CTFloat, "c" -> CTNumber, "d" -> CTAny.nullable, "e" -> CTString)

    assertExpr.from("a / a") shouldHaveInferredType CTInteger
    assertExpr.from("b / b") shouldHaveInferredType CTFloat
    assertExpr.from("a / b") shouldHaveInferredType CTFloat
    assertExpr.from("b / a") shouldHaveInferredType CTFloat
    assertExpr.from("a / c") shouldHaveInferredType CTNumber
    assertExpr.from("c / b") shouldHaveInferredType CTNumber

    assertExpr.from("a / d") shouldHaveInferredType CTAny.nullable
    assertExpr.from("d / c") shouldHaveInferredType CTAny.nullable

    assertExpr.from("a / e") shouldFailToInferTypeWithErrors
      InvalidType("e", Seq(CTInteger, CTFloat, CTNumber), CTString)
  }

  it("typing label predicates") {
    implicit val context: TypeTracker = typeTracker("n" -> CTNode())

    assertExpr.from("n:Person") shouldHaveInferredType CTBoolean
    assertExpr.from("n:Person:Car") shouldHaveInferredType CTBoolean
    assertExpr.from("NOT n:Person:Car") shouldHaveInferredType CTBoolean
    assertExpr.from("NOT(NOT(n:Person:Car))") shouldHaveInferredType CTBoolean
  }

  it("typing AND and OR") {
    implicit val context: TypeTracker = typeTracker("b" -> CTBoolean, "c" -> CTBoolean, "int" -> CTInteger, "d" -> CTBoolean.nullable)

    assertExpr.from("b AND d") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("b OR d") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("b AND true") shouldHaveInferredType CTBoolean
    assertExpr.from("b OR false") shouldHaveInferredType CTBoolean
    assertExpr.from("(b AND true) OR (b AND c)") shouldHaveInferredType CTBoolean

    Seq("b AND int", "int OR b", "b AND int AND c").foreach { s =>
      assertExpr(parseExpr(s)) shouldFailToInferTypeWithErrors InvalidType(varFor("int"), CTBoolean, CTInteger)
    }
  }

  it("can get label information through combined predicates") {
    implicit val tracker: TypeTracker = typeTracker("b" -> CTBoolean, "n" -> CTNode(), "x" -> CTString)

    assertExpr.from("b AND n:Person AND b AND n:Foo") shouldHaveInferredType CTBoolean
    assertExpr.from("b AND n:Person AND b AND n:Foo") shouldMake varFor("n") haveType CTNode("Person", "Foo")
    assertExpr.from("n.name = x AND n:Person") shouldMake varFor("n") haveType CTNode("Person")
    assertExpr.from("n.name = x AND n:Person") shouldMake prop("n", "name") haveType CTString
  }

  it("should detail entity type from predicate") {
    implicit val context: TypeTracker = typeTracker("n" -> CTNode)

    assertExpr.from("n:Person") shouldMake varFor("n") haveType CTNode("Person")
    assertExpr.from("n:Person AND n:Dog") shouldMake varFor("n") haveType CTNode("Person", "Dog")

    assertExpr.from("n:Person OR n:Dog") shouldMake varFor("n") haveType CTNode // not enough information for us to act
  }

  it("typing equality") {
    implicit val context: TypeTracker = typeTracker("n" -> CTInteger)

    assertExpr.from("n = 1") shouldHaveInferredType CTBoolean
    assertExpr.from("n <> 1") shouldHaveInferredType CTBoolean
  }

  it("typing less than") {
    implicit val context: TypeTracker = typeTracker("n" -> CTInteger, "m" -> CTFloat, "o" -> CTString)

    assertExpr.from("n < n") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("n < m") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("n < o") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("o < n") shouldHaveInferredType CTBoolean.nullable
  }

  it("typing less than or equal") {
    implicit val context: TypeTracker = typeTracker("n" -> CTInteger, "m" -> CTInteger.nullable, "o" -> CTString)

    assertExpr.from("n <= n") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("n <= m") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("n <= o") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("o <= n") shouldHaveInferredType CTBoolean.nullable
  }

  it("typing greater than") {
    implicit val context: TypeTracker = typeTracker("n" -> CTInteger, "m" -> CTInteger, "o" -> CTString)

    assertExpr.from("n > m") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("n > o") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("o > n") shouldHaveInferredType CTBoolean.nullable
  }

  it("typing greater than or equal") {
    implicit val context: TypeTracker = typeTracker("n" -> CTInteger, "m" -> CTInteger, "o" -> CTString)

    assertExpr.from("n >= m") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("n >= o") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("o >= n") shouldHaveInferredType CTBoolean.nullable
  }

  it("typing property equality and IN") {
    implicit val context: TypeTracker = typeTracker("n" -> CTNode("Person"))

    assertExpr.from("n.name = 'foo'") shouldHaveInferredType CTBoolean
    assertExpr.from("n.name IN ['foo', 'bar']") shouldHaveInferredType CTBoolean
    assertExpr.from("n.name IN 'foo'") shouldFailToInferTypeWithErrors
      InvalidType(parseExpr("'foo'"), CTList(CTWildcard), CTString)
  }

  it("typing of unsupported expressions") {
    val expr = mock[Expression]
    assertExpr(expr) shouldFailToInferTypeWithErrors UnsupportedExpr(expr)
  }

  it("typing of variables") {
    implicit val tracker: TypeTracker = typeTracker("n" -> CTNode("Person"))

    assertExpr.from("n") shouldHaveInferredType CTNode("Person")
  }

  it("typing of parameters (1)") {
    implicit val tracker: TypeTracker = TypeTracker.empty.withParameters(Map("param" -> CypherValue("foobar")))

    assertExpr.from("$param") shouldHaveInferredType CTString
  }

  it("typing of basic literals") {
    assertExpr.from("1") shouldHaveInferredType CTInteger
    assertExpr.from("-3") shouldHaveInferredType CTInteger
    assertExpr.from("true") shouldHaveInferredType CTBoolean
    assertExpr.from("false") shouldHaveInferredType CTBoolean
    assertExpr.from("null") shouldHaveInferredType CTNull
    assertExpr.from("3.14") shouldHaveInferredType CTFloat
    assertExpr.from("-3.14") shouldHaveInferredType CTFloat
    assertExpr.from("'-3.14'") shouldHaveInferredType CTString
  }

  it("typing of list literals") {
    assertExpr.from("[]") shouldHaveInferredType CTList(CTVoid)
    assertExpr.from("[1, 2]") shouldHaveInferredType CTList(CTInteger)
    assertExpr.from("[1, 1.0]") shouldHaveInferredType CTList(CTNumber)
    assertExpr.from("[1, 1.0, '']") shouldHaveInferredType CTList(CTAny)
    assertExpr.from("[1, 1.0, null]") shouldHaveInferredType CTList(CTNumber.nullable)
  }

  it("typing of list indexing") {
    implicit val context: TypeTracker = TypeTracker.empty.updated(Parameter("param", symbols.CTAny)(pos), CTInteger)

    assertExpr.from("[1, 2][15]") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("[3.14, -1, 5000][15]") shouldHaveInferredType CTNumber.nullable
    assertExpr.from("[[], 1, true][15]") shouldHaveInferredType CTAny.nullable
    assertExpr.from("[1, 2][1]") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("[3.14, -1, 5000][$param]")(TypeTracker.empty.withParameters(Map("param" -> CypherValue(42L)))) shouldHaveInferredType CTNumber.nullable
    assertExpr.from("[[], 1, true][$param]")(TypeTracker.empty.withParameters(Map("param" -> CypherValue(21L)))) shouldHaveInferredType CTAny.nullable
  }

  it("typing of map indexing") {
    implicit val context: TypeTracker = TypeTracker.empty
      .updated(Variable("map")(pos), CTMap(Map("foo" -> CTInteger, "bar" -> CTBoolean)))
      .updated(Variable("stringKey")(pos), CTString)
      .updated(Variable("intKey")(pos), CTInteger)
      .withParameters(Map(
        "stringParam" -> CypherValue("bar"),
        "intParam" -> CypherValue(42L)
      ))

    assertExpr.from("""map["foo"]""") shouldHaveInferredType CTInteger
    assertExpr.from("""map["bar"]""") shouldHaveInferredType CTBoolean
    assertExpr.from("""map["baz"]""") shouldHaveInferredType CTVoid

    assertExpr.from("""map[stringKey]""") shouldHaveInferredType CTAny.nullable
    assertExpr.from("""map[intKey]""") shouldFailToInferTypeWithErrors InvalidType(Variable("intKey")(pos), CTString, CTInteger)

    assertExpr.from("""map[$stringParam]""") shouldHaveInferredType CTBoolean
    assertExpr.from("""map[$intParam]""") shouldFailToInferTypeWithErrors InvalidType(Parameter("intParam", symbols.CTAny)(pos), CTString, CTInteger)
  }

  it("infer type of node property lookup") {
    implicit val context: TypeTracker = typeTracker("n" -> CTNode("Person"))

    assertExpr.from("n.name") shouldHaveInferredType CTString
  }

  it("infer type of relationship property lookup") {
    implicit val context: TypeTracker = typeTracker("r" -> CTRelationship("KNOWS"))

    assertExpr.from("r.relative") shouldHaveInferredType CTBoolean
  }

  it("types of id function") {
    implicit val context: TypeTracker = typeTracker("n" -> CTNode("Person"))

    assertExpr.from("id(n)") shouldHaveInferredType CTInteger.nullable
  }

  it("types functions") {
    assertExpr.from("toInteger(1.0)") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("size([0, true, []])") shouldHaveInferredType CTInteger.nullable

    assertExpr.from("percentileDisc(1, 3.14)") shouldHaveInferredType CTInteger.nullable
    assertExpr.from("percentileDisc(6.67, 3.14)") shouldHaveInferredType CTFloat.nullable
    assertExpr.from("percentileDisc([1, 3.14][0], 3.14)") shouldHaveInferredType CTNumber.nullable

    // TODO: Making this work requires union types and changing how nullability works. Sad!
    //
    // implicit val context = TyperContext.empty :+ Parameter("param", symbols.CTAny)(pos) -> CTInteger
    // assertExpr.from("percentileDisc([1, 3.14][$param], 3.14)") shouldHaveInferredType CTInteger
  }

  it("types the properties function") {
    implicit val context: TypeTracker = typeTracker(
      "person" -> CTNode(Set("Person")),
      "personFoo" -> CTNode().nullable,
      "rel" -> CTRelationship(),
      "knows" -> CTRelationship("KNOWS").nullable,
      "map" -> CTMap(Map("foo" -> CTString, "bar" -> CTInteger))
    )

    assertExpr.from("properties(person)") shouldHaveInferredType CTMap(Map("name" -> CTString, "age" -> CTInteger))
    assertExpr.from("properties(personFoo)") shouldHaveInferredType CTMap(Map("name" -> CTAny, "age" -> CTNumber)).nullable
    assertExpr.from("properties(rel)") shouldHaveInferredType CTMap(Map("since" -> CTInteger, "relative" -> CTBoolean))
    assertExpr.from("properties(knows)") shouldHaveInferredType CTMap(Map("since" -> CTInteger, "relative" -> CTBoolean)).nullable
    assertExpr.from("properties(map)") shouldHaveInferredType CTMap(Map("foo" -> CTString, "bar" -> CTInteger))
  }

  it("types the range function") {
    implicit val context: TypeTracker = typeTracker("from" -> CTInteger, "to" -> CTInteger, "step" -> CTInteger)
    assertExpr.from("range(from, to, step)") shouldHaveInferredType CTList(CTInteger).nullable
  }

  it("types the range function with nullable") {
    implicit val context: TypeTracker = typeTracker("from" -> CTInteger.nullable, "to" -> CTInteger)
    assertExpr.from("range(from, to)") shouldHaveInferredType CTList(CTInteger).nullable
  }

  it("types STARTS WITH, CONTAINS, ENDS WITH") {
    implicit val context: TypeTracker = typeTracker("string" -> CTString, "r" -> CTString)
    assertExpr.from("string STARTS WITH r") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("string ENDS WITH r") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("string CONTAINS r") shouldHaveInferredType CTBoolean.nullable
  }

  it("types STARTS WITH, CONTAINS, ENDS WITH with null test") {
    implicit val context: TypeTracker = typeTracker("string" -> CTString, "r" -> CTNull)
    assertExpr.from("string STARTS WITH r") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("string ENDS WITH r") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("string CONTAINS r") shouldHaveInferredType CTBoolean.nullable
  }

  it("types STARTS WITH, CONTAINS, ENDS WITH with null string") {
    implicit val context: TypeTracker = typeTracker("string" -> CTNull, "r" -> CTString)
    assertExpr.from("string STARTS WITH r") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("string ENDS WITH r") shouldHaveInferredType CTBoolean.nullable
    assertExpr.from("string CONTAINS r") shouldHaveInferredType CTBoolean.nullable
  }

  private def typeTracker(typings: (String, CypherType)*): TypeTracker =
    TypeTracker(List(typings.map { case (v, t) => varFor(v) -> t }.toMap))

  private object assertExpr {
    def from(exprText: String)(implicit tracker: TypeTracker = TypeTracker.empty) =
      assertExpr(parseExpr(exprText))
  }

  private case class assertExpr(expr: Expression)(implicit val tracker: TypeTracker = TypeTracker.empty) {

    def shouldMake(inner: Expression): Object {
      def haveType(t: CypherType): Assertion

      val inferredTypes: TypeTracker
    } = new {
      val inferredTypes: TypeTracker = typer.inferOrThrow(expr, tracker).tracker
      def haveType(t: CypherType): Assertion = {
        inferredTypes.get(inner) should equal(Some(t))
      }
    }

    def shouldHaveInferredType(expected: CypherType): Assertion = {
      val result = typer.inferOrThrow(expr, tracker)
      result.value shouldBe expected
    }

    def shouldFailToInferTypeWithErrors(expectedHd: TyperError, expectedTail: TyperError*): Assertion = {
      typer.infer(expr, tracker) match {
        case Left(actual) =>
          actual.toList.toSet should equal(NonEmptyList.of(expectedHd, expectedTail: _*).toList.toSet)
        case _ =>
          fail("Expected to get typing errors, but succeeded")
      }
    }
  }
}
