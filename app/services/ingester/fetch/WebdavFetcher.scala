
package services.ingester.fetch

import java.nio.file._

import com.github.sardine.Sardine

import scala.collection.JavaConversions._

object WebdavFetcher extends WebdavFetcher

class WebdavFetcher {

  def fetchAll(url: String, username: String, password: String, outputDirectory: Path): Long = {
    val sardine = begin(username, password)
    val resources = sardine.list(url).to[Seq]
    val map = resources.map {
      res =>
        val resUrl = res.getHref.toURL
        val in = sardine.get(resUrl.toExternalForm)
        val bytesCopied = Files.copy(in, Paths.get(outputDirectory.toString, res.getName))
        in.close()
        bytesCopied
    }
    map.sum
  }

  // test seam
  def begin(username: String, password: String): Sardine = {
    com.github.sardine.SardineFactory.begin(username, password)
  }

}

