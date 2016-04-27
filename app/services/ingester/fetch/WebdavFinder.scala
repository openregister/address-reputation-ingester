/*
 * Copyright 2016 HM Revenue & Customs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package services.ingester.fetch

import java.net.URL

import uk.co.hmrc.logging.SimpleLogger


class WebdavFinder(logger: SimpleLogger, sardine: SardineWrapper) {
  val knownProducts = List("abi", "abp")

  def findAvailable(url: URL, username: String, password: String): List[OSGBProduct] = {
    val tree = sardine.exploreRemoteTree(url, username, password)
    extractUsableProducts(tree)
  }

  private def extractUsableProducts(file: WebDavFile): List[OSGBProduct] = {
    knownProducts.flatMap {
      extractUsableProductFor(_, file)
    }
  }

  private def extractUsableProductFor(product: String, file: WebDavFile): List[OSGBProduct] = {
    val matches = file.files.filter(_.fullName == product)
    if (matches.isEmpty) Nil
    else {
      matches.head.files.flatMap {
        extractOne(product, _)
      }
    }
  }

  private def extractOne(product: String, epoch: WebDavFile): Option[OSGBProduct] = {
    val fullFolder = epoch.files.filter(_.fullName == "full")
    if (fullFolder.isEmpty) None
    else {
      val possibleDownloads: List[WebDavFile] = filterZipsWithTxt(fullFolder.head.files)
      if (possibleDownloads.isEmpty) None
      else Some(OSGBProduct(product, epoch.fullName.toInt, possibleDownloads))
    }
  }

  private def filterZipsWithTxt(files: List[WebDavFile]): List[WebDavFile] = {
    val names = files.map(_.name).toSet
    val allPairsExist = names.forall {
      n =>
        files.exists(f => f.isPlainText && f.name == n) &&
          files.exists(f => f.isZipFile && f.name == n)
    }
    if (allPairsExist)
      files.filter(_.isZipFile)
    else
      Nil
  }
}


case class OSGBProduct(productName: String, epoch: Int, zips: List[WebDavFile])

