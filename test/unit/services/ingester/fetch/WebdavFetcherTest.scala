
package services.ingester.fetch

import java.io.{File, FileInputStream}
import java.net.URI
import java.nio.file.{Files, Path, Paths}

import scala.collection.JavaConversions._
import org.mockito.Mockito._
import com.github.sardine.{DavResource, Sardine}
import org.junit.runner.RunWith
import org.scalatest.Matchers
import org.scalatest.junit.JUnitRunner
import org.scalatestplus.play.PlaySpec
import org.specs2.mock.Mockito

@RunWith(classOf[JUnitRunner])
class WebdavFetcherTest extends PlaySpec with Mockito {

  "fetch all" should {

    "copy files to output directory" in {
      // given
      val outputDirectory = Files.createTempDirectory("webdav-fetcher-test")
      val sardine = mock[Sardine]
      val url = "http://somedavserver.com/prod/rel/variant/"
      val files = new File(getClass.getResource("/webdav").toURI).listFiles().to[Seq]
      val resources = files.map(toDavResource(_, url))
      when(sardine.list(url)).thenReturn(resources.toList)
      files.foreach {
        f =>
          when(sardine.get(toUrl(url, f))).thenReturn(new FileInputStream(f))
      }
      val fetcher = new WebdavFetcher() {
        override def begin(username: String, password: String): Sardine = sardine
      }
      // when
      val total = fetcher.fetchAll(url, "foo", "bar", outputDirectory)
      // then
      total must be(files.map(_.length()).sum)
      outputDirectory.toFile.listFiles().map(_.getName()).to[Set] must be(files.map(_.getName()).to[Set])
      // finally
      deleteDir(outputDirectory)
    }

  }

  private def toDavResource(f: File, url: String): DavResource = {
    val res = mock[DavResource]
    when(res.getHref()).thenReturn(new URI(toUrl(url, f)))
    when(res.getName()).thenReturn(f.getName())
    res
  }

  private def toUrl(url: String, f: File) = s"${url}${f.getName()}"

  private def deleteDir(path: Path): Unit = {
    path.toFile.listFiles().to[Seq].foreach(f => f.delete())
    path.toFile.deleteOnExit()
  }

}
