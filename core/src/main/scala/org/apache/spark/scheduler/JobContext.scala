package org.apache.spark.scheduler

import scala.collection.mutable.HashMap

class JobContext(val jobId: Int) {
   val stageContexts: HashMap[Int, HashMap[Int, String]] = new HashMap[Int, HashMap[Int, String]]
}
