package org.apache.spark.scheduler

import scala.collection.mutable.HashMap

class SparkJobContext(val jobId: Int) extends Serializable {
   val stageContexts: HashMap[Int, HashMap[Int, String]] = new HashMap[Int, HashMap[Int, String]]

  override def toString: String = {
    val output = "JobContext, jobId:" + jobId + "stage contexts:" + stageContexts
    output
  }
}
