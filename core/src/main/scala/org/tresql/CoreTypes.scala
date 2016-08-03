package org.tresql

abstract class CoreTypes {
  type Dialect = PartialFunction[Expr, String]

  //converters
  type Converter[T] = (RowLike, Manifest[T]) => T
  implicit def convAny(r: RowLike, m: Manifest[Any]) = r(0).asInstanceOf[Any]
  implicit def convInt(r: RowLike, m: Manifest[Int]) = r.int(0)
  implicit def convLong(r: RowLike, m: Manifest[Long]) = r.long(0)
  implicit def convDouble(r: RowLike, m: Manifest[Double]) = r.double(0)
  implicit def convBoolean(r: RowLike, m: Manifest[Boolean]) = r.boolean(0)
  implicit def convBigDecimal(r: RowLike, m: Manifest[BigDecimal]) = r.bigdecimal(0)
  implicit def convString(r: RowLike, m: Manifest[String]) = r.string(0)
  implicit def convDate(r: RowLike, m: Manifest[java.util.Date]): java.util.Date = r.timestamp(0)
  implicit def convSqlDate(r: RowLike, m: Manifest[java.sql.Date]) = r.date(0)
  implicit def convSqlTimestamp(r: RowLike, m: Manifest[java.sql.Timestamp]) = r.timestamp(0)
  implicit def convJInt(r: RowLike, m: Manifest[java.lang.Integer]) = r.jInt(0)
  implicit def convJLong(r: RowLike, m: Manifest[java.lang.Long]) = r.jLong(0)
  implicit def convJDouble(r: RowLike, m: Manifest[java.lang.Double]) = r.jDouble(0)
  implicit def convJBigDecimal(r: RowLike, m: Manifest[java.math.BigDecimal]) = r.jBigDecimal(0)
  implicit def convJBoolean(r: RowLike, m: Manifest[java.lang.Boolean]) = r.jBoolean(0)
  implicit def convByteArray(r: RowLike, m: Manifest[Array[Byte]]) = r.bytes(0)
  implicit def convInputStream(r: RowLike, m: Manifest[java.io.InputStream]) = r.stream(0)
  implicit def convReader(r: RowLike, m: Manifest[java.io.Reader]) = r.reader(0)
  implicit def convBlob(r: RowLike, m: Manifest[java.sql.Blob]) = r blob 0
  implicit def convClob(r: RowLike, m: Manifest[java.sql.Clob]) = r clob 0
  //do not make Product conversion implicit since it spans also case classes
  def convTuple[T <: Product](r: RowLike, m: Manifest[T]) = if (m.toString.startsWith("scala.Tuple"))
    (m.typeArguments: @unchecked) match {
      case m1 :: Nil => (r.typed(0)(m1)).asInstanceOf[T]
      case m1 :: m2 :: Nil => (r.typed(0)(m1), r.typed(1)(m2)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: m13 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12), r.typed(12)(m13)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: m13 :: m14 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12), r.typed(12)(m13), r.typed(13)(m14)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: m13 :: m14 :: m15 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12), r.typed(12)(m13), r.typed(13)(m14), r.typed(14)(m15)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: m13 :: m14 :: m15 :: m16 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12), r.typed(12)(m13), r.typed(13)(m14), r.typed(14)(m15), r.typed(15)(m16)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: m13 :: m14 :: m15 :: m16 :: m17 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12), r.typed(12)(m13), r.typed(13)(m14), r.typed(14)(m15), r.typed(15)(m16), r.typed(16)(m17)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: m13 :: m14 :: m15 :: m16 :: m17 :: m18 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12), r.typed(12)(m13), r.typed(13)(m14), r.typed(14)(m15), r.typed(15)(m16), r.typed(16)(m17), r.typed(17)(m18)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: m13 :: m14 :: m15 :: m16 :: m17 :: m18 :: m19 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12), r.typed(12)(m13), r.typed(13)(m14), r.typed(14)(m15), r.typed(15)(m16), r.typed(16)(m17), r.typed(17)(m18), r.typed(18)(m19)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: m13 :: m14 :: m15 :: m16 :: m17 :: m18 :: m19 :: m20 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12), r.typed(12)(m13), r.typed(13)(m14), r.typed(14)(m15), r.typed(15)(m16), r.typed(16)(m17), r.typed(17)(m18), r.typed(18)(m19), r.typed(19)(m20)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: m13 :: m14 :: m15 :: m16 :: m17 :: m18 :: m19 :: m20 :: m21 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12), r.typed(12)(m13), r.typed(13)(m14), r.typed(14)(m15), r.typed(15)(m16), r.typed(16)(m17), r.typed(17)(m18), r.typed(18)(m19), r.typed(19)(m20), r.typed(20)(m21)).asInstanceOf[T]
      case m1 :: m2 :: m3 :: m4 :: m5 :: m6 :: m7 :: m8 :: m9 :: m10 :: m11 :: m12 :: m13 :: m14 :: m15 :: m16 :: m17 :: m18 :: m19 :: m20 :: m21 :: m22 :: Nil => (r.typed(0)(m1), r.typed(1)(m2), r.typed(2)(m3), r.typed(3)(m4), r.typed(4)(m5), r.typed(5)(m6), r.typed(6)(m7), r.typed(7)(m8), r.typed(8)(m9), r.typed(9)(m10), r.typed(10)(m11), r.typed(11)(m12), r.typed(12)(m13), r.typed(13)(m14), r.typed(14)(m15), r.typed(15)(m16), r.typed(16)(m17), r.typed(17)(m18), r.typed(18)(m19), r.typed(19)(m20), r.typed(20)(m21), r.typed(21)(m22)).asInstanceOf[T]
    }
  else sys.error("Cannot convert row to product of type: " + m)

  implicit def convTuple1[T <: Tuple1[_]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0))).asInstanceOf[T]
  }
  implicit def convTuple2[T <: Tuple2[_, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1))).asInstanceOf[T]
  }
  implicit def convTuple3[T <: Tuple3[_, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2))).asInstanceOf[T]
  }
  implicit def convTuple4[T <: Tuple4[_, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3))).asInstanceOf[T]
  }
  implicit def convTuple5[T <: Tuple5[_, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4))).asInstanceOf[T]
  }
  implicit def convTuple6[T <: Tuple6[_, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5))).asInstanceOf[T]
  }
  implicit def convTuple7[T <: Tuple7[_, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6))).asInstanceOf[T]
  }
  implicit def convTuple8[T <: Tuple8[_, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7))).asInstanceOf[T]
  }
  implicit def convTuple9[T <: Tuple9[_, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8))).asInstanceOf[T]
  }
  implicit def convTuple10[T <: Tuple10[_, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9))).asInstanceOf[T]
  }
  implicit def convTuple11[T <: Tuple11[_, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10))).asInstanceOf[T]
  }
  implicit def convTuple12[T <: Tuple12[_, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11))).asInstanceOf[T]
  }
  implicit def convTuple13[T <: Tuple13[_, _, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11)), r.typed(12)(a(12))).asInstanceOf[T]
  }
  implicit def convTuple14[T <: Tuple14[_, _, _, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11)), r.typed(12)(a(12)), r.typed(13)(a(13))).asInstanceOf[T]
  }
  implicit def convTuple15[T <: Tuple15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11)), r.typed(12)(a(12)), r.typed(13)(a(13)), r.typed(14)(a(14))).asInstanceOf[T]
  }
  implicit def convTuple16[T <: Tuple16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11)), r.typed(12)(a(12)), r.typed(13)(a(13)), r.typed(14)(a(14)), r.typed(15)(a(15))).asInstanceOf[T]
  }
  implicit def convTuple17[T <: Tuple17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11)), r.typed(12)(a(12)), r.typed(13)(a(13)), r.typed(14)(a(14)), r.typed(15)(a(15)), r.typed(16)(a(16))).asInstanceOf[T]
  }
  implicit def convTuple18[T <: Tuple18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11)), r.typed(12)(a(12)), r.typed(13)(a(13)), r.typed(14)(a(14)), r.typed(15)(a(15)), r.typed(16)(a(16)), r.typed(17)(a(17))).asInstanceOf[T]
  }
  implicit def convTuple19[T <: Tuple19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11)), r.typed(12)(a(12)), r.typed(13)(a(13)), r.typed(14)(a(14)), r.typed(15)(a(15)), r.typed(16)(a(16)), r.typed(17)(a(17)), r.typed(18)(a(18))).asInstanceOf[T]
  }
  implicit def convTuple20[T <: Tuple20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11)), r.typed(12)(a(12)), r.typed(13)(a(13)), r.typed(14)(a(14)), r.typed(15)(a(15)), r.typed(16)(a(16)), r.typed(17)(a(17)), r.typed(18)(a(18)), r.typed(19)(a(19))).asInstanceOf[T]
  }
  implicit def convTuple21[T <: Tuple21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11)), r.typed(12)(a(12)), r.typed(13)(a(13)), r.typed(14)(a(14)), r.typed(15)(a(15)), r.typed(16)(a(16)), r.typed(17)(a(17)), r.typed(18)(a(18)), r.typed(19)(a(19)), r.typed(20)(a(20))).asInstanceOf[T]
  }
  implicit def convTuple22[T <: Tuple22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _]](r: RowLike, m: Manifest[T]) = {
    val a = m.typeArguments
    (r.typed(0)(a(0)), r.typed(1)(a(1)), r.typed(2)(a(2)), r.typed(3)(a(3)), r.typed(4)(a(4)), r.typed(5)(a(5)), r.typed(6)(a(6)), r.typed(7)(a(7)), r.typed(8)(a(8)), r.typed(9)(a(9)), r.typed(10)(a(10)), r.typed(11)(a(11)), r.typed(12)(a(12)), r.typed(13)(a(13)), r.typed(14)(a(14)), r.typed(15)(a(15)), r.typed(16)(a(16)), r.typed(17)(a(17)), r.typed(18)(a(18)), r.typed(19)(a(19)), r.typed(20)(a(20)), r.typed(21)(a(21))).asInstanceOf[T]
  }
}

object CoreTypes extends CoreTypes
