
package controllers

import java.nio.file.{Path, Paths}

import config.ConfigHelper._
import play.api.Logger
import play.api.Play._
import play.api.mvc.{Action, AnyContent}
import services.ingester.exec.TaskFactory
import services.ingester.fetch.WebdavFetcher
import uk.co.hmrc.logging.LoggerFacade
import uk.gov.hmrc.play.microservice.controller.BaseController

object FetchController extends FetchController(
  new TaskFactory(),
  new WebdavFetcher(new LoggerFacade(Logger.logger)),
  mustGetConfigString(current.mode, current.configuration, "app.remote.server"),
  mustGetConfigString(current.mode, current.configuration, "app.remote.user"),
  mustGetConfigString(current.mode, current.configuration, "app.remote.pass"),
  Paths.get(mustGetConfigString(current.mode, current.configuration, "app.files.rootFolder"))
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
