package fs2.osm
package postgres

import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*

case class Config(jdbcUrl: String, username: String, password: String) derives ConfigReader
