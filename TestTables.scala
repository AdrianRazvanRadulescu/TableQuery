import scala.io.Source

object TestTables {
  val table1 : Table = new Table(
    List("col1", "col2"), List(
      List("a", "2"),
      List("b", "3"),
      List("c", "4"),
      List("d", "5")
    ))

  val table1String: String = {
    val src = Source.fromFile("tables/table1.csv")
    val str = src.mkString
    src.close()
    str.replace("\r", "")
  }

  val table2 : Table = {
    val src = Source.fromFile("tables/table2.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val table3 : Table = {
    val src = Source.fromFile("tables/table3.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val table4: Table = {
    val src = Source.fromFile("tables/table4.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val table3_4_merged : Table = {
    val src = Source.fromFile("tables/table3_4_merged.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val test3_newCol_Value : Table = {
    val src = Source.fromFile("tables/test_3_newCol_Value.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val test3_Select_Value: Table = {
    val src = Source.fromFile("tables/test_3_Select_Value.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val test3_Filter_Value: Table = {
    val src = Source.fromFile("tables/test_3_Filter_Value.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val test3_Merge_Value: Table = {
    val src = Source.fromFile("tables/test_3_Merge_Value.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val tableFunctional : Table = {
    val src = Source.fromFile("tables/Functional.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val tableObjectOriented : Table = {
    val src = Source.fromFile("tables/Object-Oriented.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val tableImperative : Table = {
    val src = Source.fromFile("tables/Imperative.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val ref_programmingLanguages1: Table = {
    val src = Source.fromFile("tables/test_3_1.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val ref_programmingLanguages2: Table = {
    val src = Source.fromFile("tables/test_3_2.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  val ref_programmingLanguages3: Table = {
    val src = Source.fromFile("tables/test_3_3.csv")
    val str = src.mkString
    src.close()
    Table(str.replace("\r", ""))
  }

  // 3.1
  def programmingLanguages1: Table = {
    val tableFunctionalWithNewCol = NewCol("Functional", "Yes", Value(tableFunctional)).eval.getOrElse(new Table(List(), List(List())))
    val tableObjectOrientedWithNewCol = NewCol("Object-Oriented", "Yes", Value(tableObjectOriented)).eval.getOrElse(new Table(List(), List(List())))
    val tableImperativeWithNewCol = NewCol("Imperative", "Yes", Value(tableImperative)).eval.getOrElse(new Table(List(), List(List())))

    val merge1 = Merge("Language", Value(tableFunctionalWithNewCol), Value(tableObjectOrientedWithNewCol)).eval.getOrElse(new Table(List(), List(List())))
    val merge2 = Merge("Language", Value(merge1), Value(tableImperativeWithNewCol)).eval.getOrElse(new Table(List(), List(List())))

    Select(List("Language", "Original purpose", "Other paradigms", "Functional", "Object-Oriented", "Imperative"), Value(merge2)).eval.getOrElse(new Table(List(), List(List())))
  }
  
  // 3.2
  val programmingLanguages2: Table = {
    val containingApplicationCond = Field("Original purpose", (value: String) => value.contains("Application"))
    val containingConcurrentCond = Field("Other paradigms", (value: String) => value.contains("concurrent"))

    val result = programmingLanguages1.filter(containingApplicationCond && containingConcurrentCond)
    result.getOrElse(new Table(List(), List(List())))
  }

  // 3.3
  val programmingLanguages3: Table = {
    val selectedColumns = List("Language", "Object-Oriented", "Functional")
    val result = Select(selectedColumns, Value(programmingLanguages2)).eval
    result.getOrElse(new Table(List(), List(List())))
  }

}