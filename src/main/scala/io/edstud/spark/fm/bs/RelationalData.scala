package io.edstud.spark.fm.bs

import scala.collection.immutable.List
import org.apache.spark.rdd.RDD
import breeze.linalg.SparseVector
import io.edstud.spark.DataSet

class RelationalData (
    dataset: RDD[(Double, SparseVector[Double])],
    relationSet: List[Relation]) extends DataSet (dataset.setName("%s.mappings".format(dataset.name))) {

    val mappings = inputs

    val relations = relationSet

    val meta = initMetadata()

    override protected def computeDimension(rdd: RDD[SparseVector[Double]]): Int = {
        if (!isEmpty) {
            (super.computeDimension(rdd) - relations.size) + relations.map(_.dimension).reduce(_ + _)
        } else {
            0
        }
    }

    override def cache(): RelationalData = {
        super.cache()
        relations.foreach(_.cache)

        this
    }

    override def unpersist(): RelationalData = {
        super.unpersist()
        relations.foreach(_.unpersist)

        this
    }

    private def initMetadata(): Metadata = {
        val numAttrGroups = relationSet.map(_.meta.numAttrGroups).reduce(_+_)
        //val numAttrPerGroup = relationSet.flatMap(_.meta.attrGroup)

        logInfo("Initializing Metadata...")
        val meta = new Metadata(dimension)


        meta
    }

}

object RelationalData {

    def apply(dataset: RDD[(Double, SparseVector[Double])], relations: List[Relation]): RelationalData = {
        val relationSize: Int = relations.size
        val additionalFeatures: Int = DataSet.dimension(dataset) - relationSize
        var additionalRelations: List[Relation] = List[Relation]()
        var rdd = dataset

        if (additionalFeatures > 0) {

            rdd.cache()

            for (id <- 0 until additionalFeatures) {
                val features = rdd.map(data => SparseVector.fill(1)(data._2.valueAt(relationSize + id)))
                val relation = new Relation(features).setTemporary()
                additionalRelations = additionalRelations :+ relation
            }

            rdd = rdd.zipWithIndex.map { case ((target, vector), index) =>
                val mapping = vector.mapPairs{ (index, value) =>
                    if (index >= relationSize) {
                        index
                    } else {
                        value
                    }
                }

                (target, mapping)
            }.unpersist()

        }

        new RelationalData(rdd, relations ::: additionalRelations)
    }

}
