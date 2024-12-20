package fs2.osm
package postgres

import cats.*

case class Summary(operations: Map[String, SummaryItem] = Map.empty) {
  // def this(entries: (String, SummaryItem)*) = this {
  //   entries.foldLeft(Map.empty[String, SummaryItem]) {
  //     case (agg, (key, item)) => agg + (key -> (item + agg.getOrElse(key, SummaryItem())))
  //   }
  // }

  def get(key: String): SummaryItem = operations(key)

  def + (that: Summary): Summary =
    Summary(
      operations = this.operations ++ that.operations.map {
        case (key, item) => key -> (item + this.operations.getOrElse(key, SummaryItem()))
      }
    )

  def insert(key: String, n: Int): Summary = updateItem(key, _.insert(n))
  def update(key: String, n: Int): Summary = updateItem(key, _.update(n))
  def delete(key: String, n: Int): Summary = updateItem(key, _.delete(n))

  private def updateItem(key: String, transform: SummaryItem => SummaryItem) =
    copy(operations = operations + (key -> transform(operations.getOrElse(key, SummaryItem()))))
}

case class SummaryItem(inserted: Int = 0, updated: Int = 0, deleted: Int = 0) {
  def + (that: SummaryItem): SummaryItem =
    SummaryItem(
      this.inserted + that.inserted,
      this.updated + that.updated,
      this.deleted + that.deleted
    )

  def insert(n: Int): SummaryItem = copy(inserted  = inserted + n)
  def update(n: Int): SummaryItem = copy(updated   = updated  + n)
  def delete(n: Int): SummaryItem = copy(deleted   = deleted  + n)
}

object Summary {
  val empty: Summary = Summary()
}

given Monoid[Summary] with
  def combine(x: Summary, y: Summary): Summary = x + y
  def empty:                           Summary = Summary.empty
