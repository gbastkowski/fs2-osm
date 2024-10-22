package fs2.osm
package postgres

import doobie.*
import java.net.URL
import org.apache.logging.log4j.scala.Logging
import scala.io.Source

trait Queries extends Logging {

  protected def logAndRun(resource: URL): ConnectionIO[Int] = logAndRun(Source.fromURL(resource).mkString)

  protected def logAndRun(sql: String):   ConnectionIO[Int] = logAndRun(Fragment.const(sql))

  protected def logAndRun(sql: Fragment): ConnectionIO[Int] = {
    logger.debug(sql.update.sql)
    sql.update.run
  }
}
