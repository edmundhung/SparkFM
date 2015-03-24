package io.edstud.spark.fm.bs

import org.apache.spark.rdd.RDD
import io.edstud.spark.DataSet

class Metadata (dimension: Int) {

    protected var _attrGroup = Array.fill[Int](dimension)(0)
    protected var _numAttrGroups = 1
    protected var _numAttrPerGroup =  Array.fill[Int](numAttrGroups)(dimension)

    def attrGroup: Array[Int] = _attrGroup
    def numAttrGroups: Int = _numAttrGroups
    def numAttrPerGroup(): Array[Int] = _numAttrPerGroup

    def update(groups: Array[Int]): this.type = {
        _attrGroup = groups
        reload(groups)

        this
    }

    private def reload(groups: Array[Int]) {
        _numAttrGroups = groups.max
        _numAttrPerGroup = Array.fill[Int](numAttrGroups)(0)
        groups.groupBy(id => id).mapValues(_.size).foreach { case (id, size) =>
            _numAttrPerGroup(id) = size
        }
    }

}
