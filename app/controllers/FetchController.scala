
package controllers

import java.nio.file.{Files, Path, Paths}

import play.api.mvc.{Action, AnyContent}
import services.ingester.exec.TaskFactory
import services.ingester.fetch.WebdavFetcher
import uk.gov.hmrc.play.microservice.controller.BaseController

object FetchController extends FetchController(
  new TaskFactory(), WebdavFetcher, "url", "username", "password",
  Files.createTempDirectory("fetch-controller")
)

class FetchController(taskFactory: TaskFactory,
                      webdavFetcher: WebdavFetcher,
                      url: String,
                      username: String,
                      password: String,
                      outputDirectory: Path) extends BaseController {

  def fetch(product: String, epoch: String, variant: String): Action[AnyContent] = Action {
    val task = taskFactory.task
    val path = s"$product/$epoch/$variant"
    val started = task.start(s"fetching $path", {
      val dir = outputDirectory.resolve(path)
      webdavFetcher.fetchAll(url, username, password, dir)
    })
    if (started) Ok(task.status) else Conflict(task.status)
  }

}
