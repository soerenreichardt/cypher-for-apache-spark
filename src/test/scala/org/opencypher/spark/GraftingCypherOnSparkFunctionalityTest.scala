package org.opencypher.spark

import org.opencypher.spark.api.CypherRecord
import org.opencypher.spark.api.value.CypherList
import org.opencypher.spark.impl.StdPropertyGraph

class GraftingCypherOnSparkFunctionalityTest extends StdTestSuite with TestSession.Fixture {

  import StdPropertyGraph.SupportedQueries
  import factory._

  test("all node scan") {
    val a = add(newNode)
    val b = add(newLabeledNode("Foo", "Bar"))
    val c = add(newLabeledNode("Foo"))
    add(newRelationship(a -> "KNOWS" -> b))
    add(newRelationship(a -> "KNOWS" -> a))

    val result = graph.cypher(SupportedQueries.allNodesScan)

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("n" -> a),
      CypherRecord("n" -> b),
      CypherRecord("n" -> c)
    ))
  }

  test("get all nodes and project two properties into multiple columns") {
    add(newNode.withProperties("name" -> "Mats"))
    add(newNode.withProperties("name" -> "Stefan", "age" -> 37))
    add(newNode.withProperties("age" -> 7))
    add(newNode)

    // MATCH (n) RETURN n.name, n.age
    val result = graph.cypher(SupportedQueries.allNodesScanProjectAgeName)

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("n.name" -> "Mats", "n.age" -> null),
      CypherRecord("n.name" -> "Stefan", "n.age" -> 37),
      CypherRecord("n.name" -> null, "n.age" -> 7),
      CypherRecord("n.name" -> null, "n.age" -> null)
    ))
  }

  test("get all rels of type T from nodes of label A") {
    val a1 = add(newLabeledNode("A"))
    val a2 = add(newLabeledNode("A"))
    val b1 = add(newLabeledNode("B"))
    val b2 = add(newLabeledNode("B"))
    val b3 = add(newLabeledNode("B"))
    add(newRelationship(a1 -> "A_TO_A" -> a1))
    val r1 = add(newRelationship(a1 -> "A_TO_B" -> b1))
    val r2 = add(newRelationship(a2 -> "A_TO_B" -> b1))
    val r3 = add(newRelationship(a2 -> "X" -> b2))
    add(newRelationship(b2 -> "B_TO_B" -> b3))

    // MATCH (:A)-[r]->(:B) RETURN r
    val result = graph.cypher(SupportedQueries.getAllRelationshipsOfTypeTOfLabelA)

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("r" -> r1),
      CypherRecord("r" -> r2),
      CypherRecord("r" -> r3)
    ))
  }

  test("simple union all") {
    val a = add(newLabeledNode("A").withProperties("name" -> "Mats"))
    val b = add(newLabeledNode("B").withProperties("name" -> 123))
    val ab = add(newLabeledNode("A", "B").withProperties("name" -> "Foo"))
    val c = add(newLabeledNode("C").withProperties("name" -> "Bar"))
    add(newNode)
    add(newUntypedRelationship(a -> ab))
    add(newUntypedRelationship(ab -> c))

    // MATCH (a:A) RETURN a.name AS name UNION ALL MATCH (b:B) RETURN b.name AS name
    val result = graph.cypher(SupportedQueries.simpleUnionAll)

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("name" -> "Mats"),
      CypherRecord("name" -> "Foo"),
      CypherRecord("name" -> "Foo"),
      CypherRecord("name" -> 123)
    ))
  }

  test("get all node ids sorted desc") {
    val n1 = add(newNode)
    val n2 = add(newNode.withLabels("Foo"))
    val n3 = add(newNode.withProperties("a" -> 10))
    val n4 = add(newNode)

      // MATCH (n) RETURN id(n) AS id ORDER BY id DESC
    val result = graph.cypher(SupportedQueries.allNodeIdsSortedDesc)

    result.records.collectAsScalaList should equal(List(
      CypherRecord("id" -> 4),
      CypherRecord("id" -> 3),
      CypherRecord("id" -> 2),
      CypherRecord("id" -> 1)
    ))
  }

  test("match aggregate") {
    add(newLabeledNode("A").withProperties("name" -> "Mats"))
    add(newLabeledNode("A").withProperties("name" -> "Mats"))
    add(newLabeledNode("A").withProperties("name" -> "Stefan"))
    add(newLabeledNode("A", "B").withProperties("notName" -> "foo"))

    // MATCH (a:A) RETURN collect(a.name) AS names
    val result = graph.cypher(SupportedQueries.matchAggregate)

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("names" -> CypherList(Seq("Mats", "Mats", "Stefan")))
    ))
  }

  ignore("match aggregate unwind") {
    add(newLabeledNode("A").withProperties("name" -> "Mats"))
    add(newLabeledNode("A").withProperties("name" -> "Mats"))
    add(newLabeledNode("A").withProperties("name" -> "Stefan"))
    add(newLabeledNode("A", "B").withProperties("notName" -> "foo"))

    // MATCH (a:A) WITH collect(a.name) AS names UNWIND names AS name RETURN name
    val result = graph.cypher(SupportedQueries.matchAggregateAndUnwind)

    result.records.collectAsScalaSet should equal(Set(
      CypherRecord("name" -> "Mats"),
      CypherRecord("name" -> "Mats"),
      CypherRecord("name" -> "Stefan"),
      CypherRecord("name" -> null)
    ))
  }


  //  test("all node scan on node-only graph that uses all kinds of properties") {
//    val pg = factory(createGraph2(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScan)
//    val result = cypher.map { map =>
//      map.toString()
//    }.show()
//  }
//
//  test("get all node ids") {
//    val pg = factory(createGraph1(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodeIds)
//    val result: Dataset[String] = cypher.map { map =>
//      map.toString()
//    }
//
//    result.show(false)
//  }
//
//  test("get all nodes and project two properties into multiple columns using toDS()") {
//    val pg = factory(createGraph3(_)).graph
//
//    val cypher: CypherResult = pg.cypher(SupportedQueries.allNodesScanProjectAgeName)
//    val result = cypher.map { record =>
//      (record("name"), record("age"))
//    }
//
//    result.show(false)
//  }
//
//
//  test("get all rels of type T") {
//    val pg = factory(createGraph1(_)).graph
//
//    val cypher = pg.cypher(SupportedQueries.getAllRelationshipsOfTypeT)
//
//    cypher.show()
//  }
//
//
//  test("simple union distinct") {
//    val pg = factory(createGraph3(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.simpleUnionDistinct)
//
//    result.show()
//  }
//
//  test("optional match") {
//    val pg = factory(createGraph1(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.optionalMatch)
//
//    result.show()
//  }
//
//  test("unwind") {
//    val pg = factory(createGraph1(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.unwind)
//
//    result.show()
//  }
//
//  test("match aggregate unwind") {
//    val pg = factory(createGraph3(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.matchAggregateAndUnwind)
//
//    result.show()
//  }
//
//  test("bound variable length") {
//    val pg = factory(createGraph4(_)).graph
//
//    val result: CypherResult = pg.cypher(SupportedQueries.boundVarLength)
//
//    result.show()
//  }
}
