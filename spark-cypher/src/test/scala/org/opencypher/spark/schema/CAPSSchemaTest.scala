package org.opencypher.spark.schema

import org.opencypher.okapi.api.schema.Schema
import org.opencypher.okapi.api.types.{CTBoolean, CTFloat, CTInteger, CTString}
import org.opencypher.okapi.impl.exception.SchemaException
import org.opencypher.okapi.test.BaseTestSuite
import org.opencypher.spark.schema.CAPSSchema._

class CAPSSchemaTest extends BaseTestSuite {
  it("fails when combining type conflicting schemas resulting in type ANY") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTString)
    val schema2 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "bar" -> CTInteger)

    the[SchemaException] thrownBy (schema1 ++ schema2).asCaps should have message
      "The property type 'ANY' for property 'bar' can not be stored in a Spark column. The unsupported type is specified on label combination [A]."
  }

  it("fails when combining type conflicting schemas resulting in type NUMBER") {
    val schema1 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "baz" -> CTInteger)
    val schema2 = Schema.empty
      .withNodePropertyKeys("A")("foo" -> CTString, "baz" -> CTFloat)

    the[SchemaException] thrownBy (schema1 ++ schema2).asCaps should have message
      "The property type 'NUMBER' for property 'baz' can not be stored in a Spark column. The unsupported type is specified on label combination [A]."
  }

  it("successfully verifies the empty schema") {
    noException shouldBe thrownBy(Schema.empty.asCaps)
  }

  it("successfully verifies a valid schema") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee")("name" -> CTString, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("notName" -> CTBoolean)

    noException shouldBe thrownBy(schema.asCaps)
  }

  it("fails when verifying schema with conflict on implied labels") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee", "Person")("name" -> CTString, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("name" -> CTBoolean)

    the[SchemaException] thrownBy schema.asCaps should have message
      "The property type 'ANY' for property 'name' can not be stored in a Spark column. The conflict appears between label combinations [Dog, Pet] and [Pet]."
  }

  it("fails when verifying schema with conflict on combined labels") {
    val schema = Schema.empty
      .withNodePropertyKeys("Person")("name" -> CTString)
      .withNodePropertyKeys("Employee", "Person")("name" -> CTInteger, "salary" -> CTInteger)
      .withNodePropertyKeys("Employee")("name" -> CTInteger, "salary" -> CTInteger)
      .withNodePropertyKeys("Dog", "Pet")("name" -> CTFloat)
      .withNodePropertyKeys("Pet")("notName" -> CTBoolean)

    the[SchemaException] thrownBy schema.asCaps should have message
      "The property type 'ANY' for property 'name' can not be stored in a Spark column. The conflict appears between label combinations [Person] and [Employee, Person]."
  }
}
