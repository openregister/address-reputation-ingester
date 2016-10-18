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

package helper

import java.io.File

import io.milton.config.HttpManagerBuilder
import io.milton.http.fs.{FileSystemResourceFactory, NullSecurityManager}
import io.milton.simpleton.SimpletonServer

//TODO this is hard-wired to port 8080; it needs to use an ephemeral port during testing

class WebdavStub(rootDir: String, port: Int = 8080) {
  val resourceFactory = new FileSystemResourceFactory(new File(rootDir), new NullSecurityManager(), "/")
  resourceFactory.setAllowDirectoryBrowsing(true)

  val  builder = new HttpManagerBuilder()
  builder.setEnableFormAuth(false)
  builder.setResourceFactory(resourceFactory)

  val  httpManager = builder.buildHttpManager()

  val ss = new SimpletonServer(httpManager, builder.getOuterWebdavResponseHandler, 1, 1)
  ss.setHttpPort(port)

  def start() {
    ss.start
  }

  def stop() {
    ss.stop
  }

  //TODO getPort: Int = ???
}
