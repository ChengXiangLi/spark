package org.apache.spark

import org.apache.spark.serializer.Serializer
import java.io.{FileInputStream, File}


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
    logInfo("local store dir for shuffleId:" + shuffleId + " reduceId:" + reduceId + "dir path:" + dir +
      " found files:" + files.map(f=> f.getCanonicalPath))
    val combineIter = new Iterator[Any]{
      val iters: Seq[Iterator] = files.map(file => {
        val stream = new FileInputStream(file)
        serializer.newInstance().deserializeStream(stream).asIterator
      })
      var index = 0
      def hasNext: Boolean = {
        if (iters(index).hasNext) {
          true
        } else {
          while (index < iters.size) {
            index += 1
            if (iters(index).hasNext) {
              true
            }
          }
          false
        }
      }

      def next(): Any = {
        iters(index).next()
      }
    }

    new InterruptibleIterator[T](context, combineIter.asInstanceOf[Iterator[T]])
  }

  override def stop {

  }
}