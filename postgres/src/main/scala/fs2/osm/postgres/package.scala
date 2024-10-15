package fs2.osm

import doobie.*
import doobie.implicits.*
import doobie.postgres.circe.json.implicits.*
import io.circe.Json
import io.circe.syntax.*

package object postgres {

  extension (tags: Map[String, String]) def toJson: Json = Json.obj(tags.mapValues { _.asJson }.toSeq*)

  given Get[Map[String, String]] = Get[Json].map { json =>
    json.asObject
      .map { _.toMap.flatMap { (k, v) => v.asString.map { k -> _ } } }
      .getOrElse(Map.empty)
  }
  given Put[Map[String, String]] = Put[Json].contramap(_.asJson)

}
