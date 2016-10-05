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

package fetch

import java.net.URL

import services.model.StatusLogger
import uk.gov.hmrc.logging.Stdout

// for manual test/development
object SardineWrapperSketch {

  def main(args: Array[String]) {
    val status = new StatusLogger(Stdout)
    if (args.length > 2) {
      val finder = new SardineWrapper(new URL(args(0)), args(1), args(2), None, new SardineFactory2)
      val top = finder.exploreRemoteTree
      Stdout.info(top.toString)
    } else if (args.length > 0) {
      val finder = new SardineWrapper(new URL(args(0)), "", "", None, new SardineFactory2)
      val top = finder.exploreRemoteTree
      Stdout.info(top.toString)
    }
  }
}
