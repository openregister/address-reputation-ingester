
package controllers

import java.nio.file.{Files, Path, Paths}

import org.scalatestplus.play.{OneAppPerSuite, PlaySpec}
import org.specs2.mock.Mockito
import org.mockito.Mockito._
import play.api.test.FakeRequest
import play.api.test.Helpers._
import services.ingester.exec.{Task, TaskFactory}
import services.ingester.fetch.WebdavFetcher
import uk.co.hmrc.logging.StubLogger

class FetchControllerTest extends PlaySpec with Mockito with OneAppPerSuite {

  class MockTask extends Task(new StubLogger()) {

    var theBody = {}
    var theCleanup = {}
    var started = false

    override def status = "complete"

    override def isBusy: Boolean = false

    override def notIdle: Boolean = false

    override def awaitCompletion(): Unit = {}

    override def abort(): Boolean = true

    override def start(work: String, body: => Unit, cleanup: => Unit): Boolean = {
      theBody = body
      theCleanup = cleanup
      started = true
      started
    }

  }

  trait action {
    val testTask = new Task(new StubLogger())
    val taskFactory = new TaskFactory {
      override def task = testTask
    }
    val webdavFetcher = mock[WebdavFetcher]
    val url = "http://localhost/webdav"
    val username = "foo"
    val password = "bar"
    val outputDirectory: Path = Files.createTempDirectory("fetch-controller-test")
    val controller = new FetchController(taskFactory, webdavFetcher, url, username, password, outputDirectory)
    val req = FakeRequest()

    def teardown() {
      Files.delete(outputDirectory)
    }

  }

  "fetch" should {

    "download files using webdav" in new action {
      val product = "product"
      val epoch = "epoch"
      val variant = "variant"
      val res = call(controller.fetch(product, epoch, variant), req)
      status(res) must be (200)
      testTask.awaitCompletion()
      verify(webdavFetcher).fetchAll(url, username, password, Paths.get(outputDirectory.toString, product, epoch, variant))
      teardown()
    }

  }

}
