package controllers

import java.nio.file.{Path, Paths}

import config.ConfigHelper._
import play.api.Logger
import play.api.Play._
import play.api.mvc.{Action, AnyContent}
import services.ingester.exec.WorkerFactory
import services.ingester.fetch.{SardineFactory2, WebdavFetcher}
import uk.co.hmrc.logging.LoggerFacade
import uk.gov.hmrc.play.microservice.controller.BaseController

object FetchController extends FetchController(
  new WorkerFactory(),
  new WebdavFetcher(new LoggerFacade(Logger.logger), new SardineFactory2),
  mustGetConfigString(current.mode, current.configuration, "app.remote.server"),
  mustGetConfigString(current.mode, current.configuration, "app.remote.user"),
  mustGetConfigString(current.mode, current.configuration, "app.remote.pass"),
  Paths.get(mustGetConfigString(current.mode, current.configuration, "app.files.rootFolder"))
)

class FetchController(taskFactory: WorkerFactory,
                      webdavFetcher: WebdavFetcher,
                      url: String,
                      username: String,
                      password: String,
                      outputDirectory: Path) extends BaseController {

  def fetch(product: String, epoch: Int, variant: String): Action[AnyContent] = Action {
    val worker = taskFactory.worker
    val path = s"$product/$epoch/$variant"
    val started = worker.push(s"fetching $path", {
      val dir = outputDirectory.resolve(path)
      webdavFetcher.fetchAll(s"$url/$path", username, password, dir)
    })
    if (started) Ok(worker.status) else Conflict(worker.status)
  }

}
