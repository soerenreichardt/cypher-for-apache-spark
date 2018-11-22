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
package org.opencypher.spark.schema

import org.opencypher.okapi.api.schema.LabelPropertyMap.LabelPropertyMap
import org.opencypher.okapi.api.schema.PropertyKeys.PropertyKeys
import org.opencypher.okapi.api.schema.RelTypePropertyMap.RelTypePropertyMap
import org.opencypher.okapi.api.schema.{Schema, SchemaPattern}
import org.opencypher.okapi.api.types.{CTRelationship, CypherType}
import org.opencypher.okapi.impl.exception.{SchemaException, UnsupportedOperationException}
import org.opencypher.okapi.impl.schema.{ImpliedLabels, LabelCombinations}
import org.opencypher.spark.impl.convert.SparkConversions._

object CAPSSchema {
  val empty: CAPSSchema = Schema.empty.asCaps

  implicit class CAPSSchemaConverter(schema: Schema) {

    /**
      * Converts a given schema into a CAPS specific schema. The conversion fails if the schema specifies property types
      * that cannot be represented in CAPS and throws a [[org.opencypher.okapi.impl.exception.SchemaException]].
      */
    def asCaps: CAPSSchema = {
      schema match {
        case s: CAPSSchema => s
        case s: Schema =>
          val combosByLabel = schema.labels.map(label => label -> s.labelCombinations.combos.filter(_.contains(label)))

          combosByLabel.foreach {
            case (_, combos) =>
              val keysForAllCombosOfLabel = combos.map(combo => combo -> schema.nodePropertyKeys(combo))
              for {
                (combo1, keys1) <- keysForAllCombosOfLabel
                (combo2, keys2) <- keysForAllCombosOfLabel
              } yield {
                (keys1.keySet intersect keys2.keySet).foreach { k =>
                  val t1 = keys1(k)
                  val t2 = keys2(k)
                  val join = t1.join(t2)
                  if (!join.isSparkCompatible) {
                    val explanation = if (combo1 == combo2) {
                      s"The unsupported type is specified on label combination ${combo1.mkString("[", ", ", "]")}."
                    } else {
                      s"The conflict appears between label combinations ${combo1.mkString("[", ", ", "]")} and ${combo2.mkString("[", ", ", "]")}."
                    }
                    throw SchemaException(s"The property type '$join' for property '$k' can not be stored in a Spark column. " + explanation)
                  }
                }
              }
          }

          new CAPSSchema(s)

        case other => throw UnsupportedOperationException(s"${other.getClass.getSimpleName} does not have Tag support")

      }
    }
  }

}

case class CAPSSchema private[schema](schema: Schema) extends Schema {

  override def labels: Set[String] = schema.labels

  override def nodeKeys: Map[String, Set[String]] = schema.nodeKeys

  override def relationshipTypes: Set[String] = schema.relationshipTypes

  override def relationshipKeys: Map[String, Set[String]] = schema.relationshipKeys

  override def labelPropertyMap: LabelPropertyMap = schema.labelPropertyMap

  override def relTypePropertyMap: RelTypePropertyMap = schema.relTypePropertyMap

  override def schemaPatterns: Set[SchemaPattern] = schema.schemaPatterns

  override def withSchemaPatterns(patterns: SchemaPattern*): Schema = schema.withSchemaPatterns(patterns: _*)

  override def impliedLabels: ImpliedLabels = schema.impliedLabels

  override def labelCombinations: LabelCombinations = schema.labelCombinations

  override def impliedLabels(knownLabels: Set[String]): Set[String] = schema.impliedLabels(knownLabels)

  override def nodePropertyKeys(labels: Set[String]): PropertyKeys = schema.nodePropertyKeys(labels)

  override def allCombinations: Set[Set[String]] = schema.allCombinations

  override def combinationsFor(knownLabels: Set[String]): Set[Set[String]] = schema.combinationsFor(knownLabels)

  override def nodePropertyKeyType(labels: Set[String], key: String): Option[CypherType] = schema.nodePropertyKeyType(labels, key)

  override def nodePropertyKeysForCombinations(labelCombinations: Set[Set[String]]): PropertyKeys = schema.nodePropertyKeysForCombinations(labelCombinations)

  override def relationshipPropertyKeyType(types: Set[String], key: String): Option[CypherType] = schema.relationshipPropertyKeyType(types, key)

  override def relationshipPropertyKeys(typ: String): PropertyKeys = schema.relationshipPropertyKeys(typ)

  override def relationshipPropertyKeysForTypes(knownTypes: Set[String]): PropertyKeys = schema.relationshipPropertyKeysForTypes(knownTypes)

  override def withNodePropertyKeys(nodeLabels: Set[String], keys: PropertyKeys): Schema = schema.withNodePropertyKeys(nodeLabels, keys)

  override def withRelationshipPropertyKeys(typ: String, keys: PropertyKeys): Schema = schema.withRelationshipPropertyKeys(typ, keys)

  override def ++(other: Schema): Schema = schema ++ other

  override def pretty: String = schema.pretty

  override def isEmpty: Boolean = schema.isEmpty

  override def forNode(labelConstraints: Set[String]): Schema = schema.forNode(labelConstraints)

  override def forRelationship(relType: CTRelationship): Schema = schema.forRelationship(relType)

  override def dropPropertiesFor(combo: Set[String]): Schema = schema.dropPropertiesFor(combo)

  override def withOverwrittenNodePropertyKeys(nodeLabels: Set[String], propertyKeys: PropertyKeys): Schema = schema.withOverwrittenNodePropertyKeys(nodeLabels, propertyKeys)

  override def withOverwrittenRelationshipPropertyKeys(relType: String, propertyKeys: PropertyKeys): Schema = schema.withOverwrittenRelationshipPropertyKeys(relType, propertyKeys)

  override def toJson: String = schema.toJson

  override def explicitSchemaPatterns: Set[SchemaPattern] = schema.explicitSchemaPatterns

  override def schemaPatternsFor(knownSourceLabels: Set[String], knownRelTypes: Set[String], knownTargetLabels: Set[String]): Set[SchemaPattern] = schema.schemaPatternsFor(knownSourceLabels, knownRelTypes, knownTargetLabels)

  override def withNodeKey(label: String, nodeKey: Set[String]): Schema = schema.withNodeKey(label, nodeKey)

  override def withRelationshipKey(relationshipType: String, relationshipKey: Set[String]): Schema = schema.withRelationshipKey(relationshipType, relationshipKey)
}
