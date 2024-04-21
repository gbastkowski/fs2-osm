package fs2.osm
package postgres

import doobie.util.fragment.Fragment

trait Feature {
  val name:             String
  def tableDefinitions: List[Table]
  def dataGenerator:    List[Fragment]
}
