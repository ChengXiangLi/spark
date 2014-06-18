package org.apache.spark.scheduler

import scala.collection.mutable.HashMap

class SparkOutputContext(val jobId: Int) extends Serializable {
   val stageOutputMapping: HashMap[Int, HashMap[Int, (String, String)]] = new HashMap[Int, HashMap[Int, (String, String)]]

  override def toString: String = {
    val output = "JobContext, jobId:" + jobId + "stage output mapping:" + stageOutputMapping
    output
  }
}
