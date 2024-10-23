package fs2.osm
package postgres

import doobie.*
import Fragment.*

object TagsFragments {
  def or(t1: (String, String), t2: (String, String)): Fragment =
    Fragments.or(const(s"tags->>'${t1._1}' = '${t1._2}'"),
                 const(s"tags->>'${t2._1}' = '${t2._2}'"))

  def and(t1: (String, String), t2: (String, String)): Fragment =
    Fragments.and(const(s"tags->>'${t1._1}' = '${t1._2}'"),
                  const(s"tags->>'${t2._1}' = '${t2._2}'"))

  def has(key: String): Fragment = const(s"tags->>'$key' IS NOT NULL")

  def is(key: String, value: String): Fragment = const(s"tags->>'$key' = '$value'")
}
