package fs2.osm
package app

import pureconfig.*
import pureconfig.generic.derivation.default.*
import pureconfig.module.catseffect.syntax.*
import sttp.client3.UriContext
import sttp.model.Uri

case class Config(uri: Uri, db: postgres.Config) derives ConfigReader

private given ConfigReader[Uri] = ConfigReader[String].map { Uri.unsafeParse }
