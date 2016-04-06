//-----------------------------------------------------------------------------
// The MIT License
//
// Copyright (c) 2012 Rick Beton <rick@bigbeeconsultants.co.uk>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.
//-----------------------------------------------------------------------------

// see https://bitbucket.org/rickb777/bee-client
//scalastyle:off

package uk.co.bigbeeconsultants.http.util

/**
  * Provides a simple diagnostic timer for evaluating execution times of sections of code.
  * The measurement precision is to the nearest microsecond.
  *
  * Simply create an instance before the code under test and observe the duration afterwards.
  *
  * This class is included here to avoid dependencies on external libraries (e.g. JodaTime)
  * for such a simple task.
  */
final class DiagnosticTimer {

  /** The time at which the timer was started, measured in arbitrary ticks. */
  val start = now

  private def now = System.nanoTime() / DiagnosticTimer.thousand

  /** The duration since the timer was started, measured in microseconds. */
  def duration: Duration = new Duration(now - start)

  /** Gets the duration since the timer was started, formatted for humans to read. */
  override def toString: String = duration.toString
}

object DiagnosticTimer {
  val thousand = 1000L
  val tenThousand = 10000L
  val hundredThousand = 100000L
  val million = thousand * thousand
  val tenMillion = 10 * million
  val hundredMillion = 100 * million
}

/**
  * Provides a simple container for time durations measured in microseconds. Possible operations include
  * summing durations together and calculating averages.
  *
  * This is a bit like the class of the same name in Joda Time, except that the resolution is microseconds
  * in this case.
  * @param microseconds a number representing a time duration; negative numbers are unusual but not prohibited.
  */
case class Duration(microseconds: Long) extends Ordered[Duration] {

  import DiagnosticTimer._

  def +(microseconds: Long): Duration = new Duration(microseconds + this.microseconds)

  def +(duration: Duration): Duration = new Duration(this.microseconds + duration.microseconds)

  def -(duration: Duration): Duration = new Duration(this.microseconds - duration.microseconds)

  def -(microseconds: Long): Duration = new Duration(this.microseconds - microseconds)

  def *(factor: Int): Duration = new Duration(this.microseconds * factor)

  def /(divisor: Int): Duration = new Duration(this.microseconds / divisor)

  def abs: Duration = if (microseconds < 0) new Duration(-microseconds) else this

  def max(other: Duration): Duration = if (this.microseconds < other.microseconds) other else this

  def min(other: Duration): Duration = if (this.microseconds < other.microseconds) this else other

  def compare(that: Duration): Int = this.microseconds.compare(that.microseconds)

  override def toString: String =
    if (microseconds >= hundredMillion) {
      (microseconds / million) + "s"
    } else if (microseconds >= tenMillion) {
      val m = microseconds / million
      val f = microseconds + 50000L - (m * million)
      m + "." + f.toString.take(1) + "s"
    } else if (microseconds >= hundredThousand) {
      val roundup = microseconds + 500
      (roundup / thousand) + "ms"
    } else if (microseconds >= tenThousand) {
      val roundup = microseconds + 50
      val th = roundup / thousand
      val f = roundup - (th * thousand)
      th + "." + f.toString.take(1) + "ms"
    } else
      microseconds + "µs"
}

object Duration {
  final val Zero = new Duration(0)
}
