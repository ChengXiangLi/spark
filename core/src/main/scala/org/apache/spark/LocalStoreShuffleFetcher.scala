package org.apache.spark

import org.apache.spark.serializer.Serializer
import java.io.{FileInputStream, File}
import org.apache.spark.io.CompressionCodec
import com.google.common.collect.Lists


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
      logInfo("local store dir for shuffleId:" + shuffleId + " reduceId:" + reduceId + "dir path:" + dir +
        " found no file")
      return new Iterator[Any] {
        def hasNext: Boolean = {
          false
        }

        def next(): Any = {
          None
        }
      }.asInstanceOf[Iterator[T]]
    }
    logInfo("local store dir for shuffleId:" + shuffleId + " reduceId:" + reduceId + "dir path:" + dir +
      " found files:" + files.map(f=> f.getCanonicalPath))
    val combineIter = new Iterator[Any]{
      val iters: Seq[Iterator[Any]] = files.map(file => {
        val stream = compressionCodec.compressedInputStream(new FileInputStream(file))
        serializer.newInstance().deserializeStream(stream).asIterator
      })
      var index = 0
      def hasNext: Boolean = {
        hasNext(index)
      }

      private def hasNext(in: Int): Boolean = {
        this.index = in
        var result: Boolean = false
        if (in < iters.size) {
          if (iters(index).hasNext) {
            result = true
          } else {
            result = hasNext(in + 1)
          }
        }
        result
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