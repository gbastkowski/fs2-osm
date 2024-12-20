package fs2.osm
package postgres

import doobie.*
import java.net.URL
import org.apache.logging.log4j.scala.Logging
import scala.io.Source
import doobie.free.connection.ConnectionOp
import cats.free.Free
import fs2.Stream

trait Queries extends Logging {

  /**
   * Finds and retrieves the first `n` results from the database based on the given SQL query fragment.
   *
   * @param sql the query to execute.
   * @param n   the maximum number of results to retrieve. Must be a positive integer.
   * @return    a `Stream` producing boolean values.
   *            Each boolean in the stream represents whether a corresponding result satisfies
   *            the query's criteria. The stream will emit up to `n` results, or fewer if the query
   *            yields less data.
   */
  protected def findFirstN(sql: Fragment, n: Int): Stream[ConnectionIO, Boolean] =
    sql.query[Boolean].stream.take(n)

  protected def logAndRun(resource: URL): ConnectionIO[Int] = logAndRun(Source.fromURL(resource).mkString)

  protected def logAndRun(sql: String):   ConnectionIO[Int] = logAndRun(Fragment.const(sql))

  protected def logAndRun(sql: Fragment): ConnectionIO[Int] = {
    logger.debug(sql.update.sql)
    sql.update.run
  }

}
