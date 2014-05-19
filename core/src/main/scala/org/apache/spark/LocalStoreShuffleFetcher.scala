package org.apache.spark

import org.apache.spark.serializer.Serializer
import java.io.File


private[spark] class LocalStoreShuffleFetcher extends ShuffleFetcher with Logging {

  override def fetch[T](
                         shuffleId: Int,
                         reduceId: Int,
                         context: TaskContext,
                         serializer: Serializer)
  : Iterator[T] = {
    val localDir = SparkEnv.get.conf.get("spark.local.dir", System.getProperty("java.io.tmpdir"))
    val dirPath = localDir + File.pathSeparator + shuffleId + File.pathSeparator + reduceId
    val dir = new File(dirPath)
    val files = dir.listFiles()
    new Iterator[Any]{
      def hasNext: Boolean = {
        false
      }

      def next(): Any = {
        null
      }
    }
    null
  }

  override def stop {

  }
}