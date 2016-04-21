
package services.ingester.fetch

import java.nio.file._

import com.github.sardine.{DavResource, Sardine}
import uk.co.hmrc.logging.SimpleLogger

import scala.collection.JavaConverters._

class WebdavFetcher(logger: SimpleLogger) {

  def fetchAll(url: String, username: String, password: String, outputDirectory: Path): Long = {
    val sardine = begin(username, password)
    val resources = sardine.list(url).asScala.toSeq
    val map = resources.map {
      fetchOneFile(_, sardine, outputDirectory)
    }
    map.sum
  }

  private def fetchOneFile(res: DavResource, sardine: Sardine, outputDirectory: Path) = {
    val resUrl = res.getHref.toURL
    val in = sardine.get(resUrl.toExternalForm)
    try {
      val bytesCopied = Files.copy(in, outputDirectory.resolve(res.getName))
      Files.createFile(outputDirectory.resolve(res.getName + ".done"))
      logger.info("Fetched " + res.getName)
      bytesCopied
    } finally {
      in.close()
    }
  }

  // test seam
  def begin(username: String, password: String): Sardine = {
    com.github.sardine.SardineFactory.begin(username, password)
  }

}

