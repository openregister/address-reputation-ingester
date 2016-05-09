/*
 *
 *  *
 *  *  * Copyright 2016 HM Revenue & Customs
 *  *  *
 *  *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  *  * you may not use this file except in compliance with the License.
 *  *  * You may obtain a copy of the License at
 *  *  *
 *  *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *  *
 *  *  * Unless required by applicable law or agreed to in writing, software
 *  *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  * See the License for the specific language governing permissions and
 *  *  * limitations under the License.
 *  *
 *
 *
 */

package services.fetch

import java.net.URL


/**
  * WebDavFile is essentially an abstraction of Sardine's DavResource,
  * pimped as a case class.
  */
case class WebDavFile(url: URL, fullName: String,
                      isDirectory: Boolean = false, isPlainText: Boolean = false, isZipFile: Boolean = false,
                      files: List[WebDavFile] = Nil) {

  val name = {
    val dot = fullName.lastIndexOf('.')
    if (dot < 0) fullName
    else fullName.substring(0, dot)
  }

  override def toString: String = indentedString("")

  private def indentedString(i: String): String = {
    val slash = if (isDirectory) "/" else ""
    val zip = if (isZipFile) " (zip)" else ""
    val txt = if (isPlainText) " (txt)" else ""
    s"$i$fullName$slash$txt$zip" +
      files.map(_.indentedString(i + "  ")).mkString("\n", "", "")
  }
}


/**
  * Wraps a tree of WebDavFiles such that the OSGBProducts can be extracted easily.
  */
case class WebDavTree(root: WebDavFile) {

  def findLatestFor(product: String): Option[OSGBProduct] = {
    val available = findAvailableFor(product).sorted
    available.reverse.headOption
  }

  def findAvailableFor(product: String): List[OSGBProduct] = {
    val matches = root.files.filter(_.fullName == product)
    if (matches.isEmpty) Nil
    else {
      val epochs = matches.head.files
      epochs.flatMap {
        extractOne(product, _)
      }
    }
  }

  private def extractOne(product: String, epoch: WebDavFile): Option[OSGBProduct] = {
    val fullFolder = epoch.files.filter(_.fullName == "full")
    if (fullFolder.isEmpty) None
    else {
      val files = fullFolder.head.files
      val possibleDownloads: List[WebDavFile] = filterZipsWithTxt(files)
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


case class OSGBProduct(productName: String, epoch: Int, zips: List[WebDavFile]) extends Ordered[OSGBProduct] {

  override def compare(that: OSGBProduct): Int = this.epoch - that.epoch
}

