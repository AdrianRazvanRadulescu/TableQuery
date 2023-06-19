import util.Util.{Line, Row}

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(this, other)
  def ||(other: FilterCond): FilterCond = Or(this, other)
  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}

case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = r.get(colName).map(predicate)
}

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    for {
      result_first <- f1.eval(r)
      result_second <- f2.eval(r)
    } yield result_first && result_second
}

case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] =
    for {
      result_first <- f1.eval(r)
      result_second <- f2.eval(r)
    } yield result_first || result_second
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = Some(t)
}
/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval.flatMap(table => table.select(columns))
  }
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval.flatMap(table => table.filter(condition))
  }
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] = {
    target.eval match {
      case Some(table) => Some(table.newCol(name, defaultVal))
      case None => None
    }
  }
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] = {
    for {
      table1 <- t1.eval
      table2 <- t2.eval
      merged <- table1.merge(key, table2)
    } yield merged
  }
}


class Table (columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames : Line = columnNames
  def getTabular : List[List[String]] = tabular

  // 1.1
  override def toString: String = {
    val header = columnNames.mkString(",")
    val rows = tabular.map(row => row.mkString(",")).mkString("\n")
    header + "\n" + rows
  }

  // 2.1
  def select(columns: Line): Option[Table] = {
    if (columns.forall(column => columnNames.contains(column))) {
      val selectedIndices = columnNames.zipWithIndex.filter { case (col, _) =>
        columns.contains(col)
      }.map { case (col, index) =>
        index
      }
      
      val data = tabular.map(line => selectedIndices.map(index => line(index)))
      
      Some(new Table(columns, data))
    } else {
      None
    }
  }

  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
  
    def rowToMap(row: Line): Map[String, String] = columnNames.zip(row).toMap

    def filterRow(row: Map[String, String]): Option[Map[String, String]] = {
      cond.eval(row).flatMap(result => if (result) Some(row) else None)
    }

    val rows = tabular.map(rowToMap)
    val filteredRows = rows.flatMap(filterRow)

    if (filteredRows.length == rows.length || filteredRows.nonEmpty) {
      val data = filteredRows.map(row => columnNames.map(col => row(col)))
      Some(new Table(columnNames, data))
    } else {
      None
    }
  }

  // 2.3.
  def newCol(name: String, defaultVal: String): Table  = {
    val newColumns = columnNames :+ name
    val newData = tabular.map(row => row :+ defaultVal)
    new Table(newColumns, newData)
  }

  // 2.4
  def merge(key: String, other: Table): Option[Table] = {
    val columnNamesFirst = getColumnNames
    val columnNamesSecond = other.getColumnNames

    if (columnNamesFirst.contains(key) && columnNamesSecond.contains(key)) {
      val columnNames = (columnNamesFirst ++ columnNamesSecond).distinct
      val keyIndex1 = columnNamesFirst.indexOf(key)
      val keyIndex2 = columnNamesSecond.indexOf(key)

      val keyColumnValues = (getTabular.map(row => row(keyIndex1)) ++ other.getTabular.map(row => row(keyIndex2))).distinct

      val mergedRows = keyColumnValues.map { keyValue =>
        val rowFromTable1 = getTabular.find(_(keyIndex1) == keyValue)
        val rowFromTable2 = other.getTabular.find(_(keyIndex2) == keyValue)

        columnNames.map { columnName =>
          val columnIndex1 = columnNamesFirst.indexOf(columnName)
          val columnIndex2 = columnNamesSecond.indexOf(columnName)

          (columnIndex1, columnIndex2, rowFromTable1, rowFromTable2) match {
            case (idx1, idx2, Some(r1), Some(r2)) if idx1 != -1 && idx2 != -1 && r1(idx1) != r2(idx2) => r1(idx1) + ";" + r2(idx2)
            case (idx1, _, Some(r1), _) if idx1 != -1 => r1(idx1)
            case (_, idx2, _, Some(r2)) if idx2 != -1 => r2(idx2)
            case _ => ""
          }
        }
      }
      Some(new Table(columnNames, mergedRows))
    } else {
      None
    }
  }
}

object Table {
  // 1.2
  def apply(s: String): Table = {
    val rows = s.split("\n").toList
    val columns = rows.head.split(",", -1).toList
    val tableData = rows.tail.map(row => row.split(",", -1).toList)
    new Table(columns, tableData)
  }
}
