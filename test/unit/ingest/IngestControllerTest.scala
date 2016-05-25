/*
 *
 *  * Copyright 2016 HM Revenue & Customs
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package ingest

import ingest.writers._
import services.exec.{Continuer, WorkQueue, WorkerFactory}
import services.model.{StateModel, StatusLogger}


//class StubOutputDBWriterFactory(w: OutputDBWriter) extends OutputDBWriterFactory {
//  override def writer(model: StateModel, statusLogger: StatusLogger, settings: WriterSettings): OutputDBWriter = w
//}

//class StubIngesterFactory(i: Ingester) extends IngesterFactory {
//  override def ingester(continuer: Continuer, model: StateModel, statusLogger: StatusLogger): Ingester = i
//}

class StubWorkerFactory(w: WorkQueue) extends WorkerFactory {
  override def worker = w
}

class StubContinuer extends Continuer {
  override def isBusy: Boolean = true
}
