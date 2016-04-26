
package services.ingester.fetch

import java.net.URL
import java.nio.file._

import com.github.sardine.{DavResource, Sardine}
import uk.co.hmrc.logging.SimpleLogger

import scala.collection.JavaConverters._

class WebdavFetcher(logger: SimpleLogger, factory: SardineFactory2) {

  def fetchAll(url: String, username: String, password: String, outputDirectory: Path): Long = {
    if (!Files.exists(outputDirectory)) {
      Files.createDirectories(outputDirectory)
    }
    val sardine = factory.begin(username, password)
    val resources = sardine.list(url).asScala
    val map = resources.filterNot(_.isDirectory).map {
      fetchOneFile(url, _, sardine, outputDirectory)
    }
    map.sum
  }

  private def fetchOneFile(url: String, res: DavResource, sardine: Sardine, outputDirectory: Path) = {
    val href = res.getHref
    val myUrl = new URL(url)
    val absoluteUrl = new URL(myUrl.getProtocol, myUrl.getHost, myUrl.getPort, href.getPath)
    val in = sardine.get(absoluteUrl.toExternalForm)
    try {
      val out = outputDirectory.resolve(res.getName)
      val bytesCopied = Files.copy(in, out)
      Files.createFile(outputDirectory.resolve(res.getName + ".done"))
      logger.info("Fetched " + res.getName)
      bytesCopied
    } finally {
      in.close()
    }
  }
}

