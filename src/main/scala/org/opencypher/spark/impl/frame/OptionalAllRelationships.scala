package org.opencypher.spark.impl.frame

import org.apache.spark.sql.Dataset
import org.opencypher.spark.api.types.CTRelationship
import org.opencypher.spark.api.value.{CypherNode, CypherRelationship}
import org.opencypher.spark.impl.{PlanningContext, RelationshipFrame, StdCypherFrame, StdFrameSignature}

object OptionalAllRelationships {

  def apply(relationship: Symbol)(implicit context: PlanningContext): StdCypherFrame[CypherRelationship] = {
    val (_, sig) = StdFrameSignature.empty.addField(relationship -> CTRelationship.nullable)

    AllRelationshipsOrNull(context.relationships)(sig)
  }

  private final case class AllRelationshipsOrNull(input: Dataset[CypherRelationship])(sig: StdFrameSignature)
    extends RelationshipFrame(sig) {

    override def execute(implicit context: RuntimeContext): Dataset[CypherRelationship] = {
      if (input.rdd.isEmpty())
        context.session.createDataset[CypherRelationship](Seq(null))(context.cypherRelationshipEncoder)
      else input
    }
  }
}
