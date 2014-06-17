package org.apache.spark

import org.apache.spark.serializer.Serializer
import java.io.{FileInputStream, File}
import org.apache.spark.io.CompressionCodec


private[spark] class LocalStoreShuffleFetcher extends ShuffleFetcher with Logging {

  override def fetch[T](
                         shuffleId: Int,
                         reduceId: Int,
                         context: TaskContext,
                         serializer: Serializer)
  : Iterator[T] = {
    val localDir = SparkEnv.get.conf.get("spark.local.dir", System.getProperty("java.io.tmpdir"))
    val compressionCodec: CompressionCodec = CompressionCodec.createCodec(SparkEnv.get.conf)
    val pathSeparator = System.getProperties.getProperty("path.deparator", "/")
    val dirPath = localDir + pathSeparator + shuffleId + pathSeparator + reduceId
    val dir = new File(dirPath)
    logInfo("try to read file from " + dirPath)
    val files = dir.listFiles()
    if (files == null || files.isEmpty) {
      return new Iterator[Any] {
        def hasNext: Boolean = {
          false
        }

        def next(): Any = {
          None
        }
      }.asInstanceOf[Iterator[T]]
    }
//    logInfo("local store dir for shuffleId:" + shuffleId + " reduceId:" + reduceId + "dir path:" + dir +
//      " found files:" + files.map(f=> f.getCanonicalPath))
    val combineIter = new Iterator[Any]{
      val iters: Seq[Iterator[Any]] = files.map(file => {
        val stream = compressionCodec.compressedInputStream(new FileInputStream(file))
        serializer.newInstance().deserializeStream(stream).asIterator
      })
      var index = 0
      def hasNext: Boolean = {
        if (iters(index).hasNext) {
          true
        } else {
          while (index < iters.size - 1) {
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