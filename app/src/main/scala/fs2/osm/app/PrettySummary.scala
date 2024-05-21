package fs2.osm
package app

import fs2.osm.core.*
import fs2.osm.postgres.{Summary, SummaryItem}
import java.text.NumberFormat
import java.time.*

case class PrettySummary(summary: Summary, duration: Duration) {
  override def toString: String =
    val formattedDuration = (duration.toSeconds / 3600, duration.toSeconds / 60, duration.toSeconds % 60) match {
      case (0,     0,       seconds) => s"$seconds seconds"
      case (0,     minutes, seconds) => s"$minutes minutes, $seconds seconds"
      case (hours, minutes, seconds) => s"$hours hours, $minutes minutes, $seconds seconds"
    }

    val columnWidth =
      Set(
        summary.operations.keySet.map(_.length).max,
        NumberFormat.getInstance().format(summary.operations.values.map { _.inserted }.max).length,
        NumberFormat.getInstance().format(summary.operations.values.map { _.updated  }.max).length,
        NumberFormat.getInstance().format(summary.operations.values.map { _.deleted  }.max).length,
        "elapsed time:".length
      ).max

    def prefixPad(i: Int) = String.format("%" + columnWidth + "s", NumberFormat.getInstance().format(i))

    def row(columns: List[String], padding: Char, separator: Char) =
      columns
        .map { _.padTo(columnWidth, padding) }
        .map { padding + _ + padding }
        .mkString(separator.toString)

    List(
      List(
        row(List(" ", "inserted", "updated", "deleted"), ' ', '|'),
        row(List("-", "-", "-", "-"), '-', '+')
      ),
      summary.operations.toList.map {
        case (key, SummaryItem(inserted, updated, deleted)) =>
          row(List(key, prefixPad(inserted), prefixPad(updated), prefixPad(deleted)), ' ', '|')
      },
      List(
        row(List("-", "-", "-", "-"), '-', '+'),
        row(List("elapsed time:", s"$formattedDuration"), ' ', '|')
      )
    )
      .flatten
      .map { "  " + _ }
      .mkString("\n")
}
