package org.tresql

abstract class CoreTypes {
  type Dialect = PartialFunction[Expr, String]

  type RowConverter[T] = RowLike => T //is used in macro for selects to generate typed row objects

  //converters
  type Converter[T] = (RowLike, Int) => T
  // do not make any converter implicit to avoid ambiguity
  def convAny(r: RowLike, i: Int): Any = r(i).asInstanceOf[Any]
  implicit def convInt(r: RowLike, i: Int): Int = r.int(i)
  implicit def convLong(r: RowLike, i: Int): Long = r.long(i)
  implicit def convDouble(r: RowLike, i: Int): Double = r.double(i)
  implicit def convBoolean(r: RowLike, i: Int): Boolean = r.boolean(i)

  implicit def convBigDecimal(r: RowLike, i: Int): BigDecimal = r.bigdecimal(i)
  implicit def convBigInt(r: RowLike, i: Int): BigInt = r.bigint(i)
  implicit def convString(r: RowLike, i: Int): String = r.string(i)
  implicit def convSqlDate(r: RowLike, i: Int): java.sql.Date = r.date(i)
  implicit def convSqlTimestamp(r: RowLike, i: Int): java.sql.Timestamp = r.timestamp(i)
  implicit def convSqlTime(r: RowLike, i: Int): java.sql.Time = r.time(i)

  implicit def convLocalDate(r: RowLike, i: Int): java.time.LocalDate = Option(r.date(i)).map(_.toLocalDate).orNull
  implicit def convLocalDatetime(r: RowLike, i: Int): java.time.LocalDateTime = Option(r.timestamp(i)).map(_.toLocalDateTime).orNull
  implicit def convLocalTime(r: RowLike, i: Int): java.time.LocalTime = Option(r.time(i)).map(_.toLocalTime).orNull
  implicit def convJInt(r: RowLike, i: Int): java.lang.Integer = r.jInt(i)
  implicit def convJLong(r: RowLike, i: Int): java.lang.Long = r.jLong(i)
  implicit def convJDouble(r: RowLike, i: Int): java.lang.Double = r.jDouble(i)
  implicit def convJBigDecimal(r: RowLike, i: Int): java.math.BigDecimal = r.jBigDecimal(i)
  implicit def convJBigInteger(r: RowLike, i: Int): java.math.BigInteger = r.jBigInteger(i)
  implicit def convJBoolean(r: RowLike, i: Int): java.lang.Boolean = r.jBoolean(i)

  implicit def convByteArray(r: RowLike, i: Int): Array[Byte] = r.bytes(i)
  implicit def convInputStream(r: RowLike, i: Int): java.io.InputStream = r.stream(i)
  implicit def convReader(r: RowLike, i: Int): java.io.Reader = r.reader(i)
  implicit def convBlob(r: RowLike, i: Int): java.sql.Blob = r blob i
  implicit def convClob(r: RowLike, i: Int): java.sql.Clob = r clob i
  implicit def convArray(r: RowLike, i: Int): java.sql.Array = r array i
  implicit def convUnit(r: RowLike, i: Int): Unit = ()

  implicit def convResult(r: RowLike, i: Int): Result[_ <: RowLike] = r.result(i)
  implicit def convList[A: Converter](r: RowLike, i: Int): List[A] = {
    val c = implicitly[Converter[A]]
    r.result(i).map(c(_, 0)).toList
  }

  implicit def convTuple2[A1: Converter, A2: Converter](r: RowLike, i: Int): (A1, A2) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    (c1(r, 0), c2(r, 1))
  }

  implicit def convTuple3[A1: Converter, A2: Converter, A3: Converter](r: RowLike, i: Int): (A1, A2, A3) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    (c1(r, 0), c2(r, 1), c3(r, 2))
  }

  implicit def convTuple4[A1: Converter, A2: Converter, A3: Converter, A4: Converter](r: RowLike, i: Int): (A1, A2, A3, A4) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3))
  }

  implicit def convTuple5[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4))
  }

  implicit def convTuple6[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5))
  }

  implicit def convTuple7[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6))
  }

  implicit def convTuple8[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7))
  }

  implicit def convTuple9[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8))
  }

  implicit def convTuple10[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9))
  }

  implicit def convTuple11[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10))
  }

  implicit def convTuple12[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11))
  }

  implicit def convTuple13[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter, A13: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    val c13 = implicitly[Converter[A13]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11), c13(r, 12))
  }

  implicit def convTuple14[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter, A13: Converter, A14: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    val c13 = implicitly[Converter[A13]]
    val c14 = implicitly[Converter[A14]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11), c13(r, 12), c14(r, 13))
  }

  implicit def convTuple15[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter, A13: Converter, A14: Converter, A15: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    val c13 = implicitly[Converter[A13]]
    val c14 = implicitly[Converter[A14]]
    val c15 = implicitly[Converter[A15]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11), c13(r, 12), c14(r, 13), c15(r, 14))
  }

  implicit def convTuple16[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter, A13: Converter, A14: Converter, A15: Converter, A16: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    val c13 = implicitly[Converter[A13]]
    val c14 = implicitly[Converter[A14]]
    val c15 = implicitly[Converter[A15]]
    val c16 = implicitly[Converter[A16]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11), c13(r, 12), c14(r, 13), c15(r, 14), c16(r, 15))
  }

  implicit def convTuple17[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter, A13: Converter, A14: Converter, A15: Converter, A16: Converter, A17: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    val c13 = implicitly[Converter[A13]]
    val c14 = implicitly[Converter[A14]]
    val c15 = implicitly[Converter[A15]]
    val c16 = implicitly[Converter[A16]]
    val c17 = implicitly[Converter[A17]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11), c13(r, 12), c14(r, 13), c15(r, 14), c16(r, 15), c17(r, 16))
  }

  implicit def convTuple18[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter, A13: Converter, A14: Converter, A15: Converter, A16: Converter, A17: Converter, A18: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    val c13 = implicitly[Converter[A13]]
    val c14 = implicitly[Converter[A14]]
    val c15 = implicitly[Converter[A15]]
    val c16 = implicitly[Converter[A16]]
    val c17 = implicitly[Converter[A17]]
    val c18 = implicitly[Converter[A18]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11), c13(r, 12), c14(r, 13), c15(r, 14), c16(r, 15), c17(r, 16), c18(r, 17))
  }

  implicit def convTuple19[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter, A13: Converter, A14: Converter, A15: Converter, A16: Converter, A17: Converter, A18: Converter, A19: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    val c13 = implicitly[Converter[A13]]
    val c14 = implicitly[Converter[A14]]
    val c15 = implicitly[Converter[A15]]
    val c16 = implicitly[Converter[A16]]
    val c17 = implicitly[Converter[A17]]
    val c18 = implicitly[Converter[A18]]
    val c19 = implicitly[Converter[A19]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11), c13(r, 12), c14(r, 13), c15(r, 14), c16(r, 15), c17(r, 16), c18(r, 17), c19(r, 18))
  }

  implicit def convTuple20[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter, A13: Converter, A14: Converter, A15: Converter, A16: Converter, A17: Converter, A18: Converter, A19: Converter, A20: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    val c13 = implicitly[Converter[A13]]
    val c14 = implicitly[Converter[A14]]
    val c15 = implicitly[Converter[A15]]
    val c16 = implicitly[Converter[A16]]
    val c17 = implicitly[Converter[A17]]
    val c18 = implicitly[Converter[A18]]
    val c19 = implicitly[Converter[A19]]
    val c20 = implicitly[Converter[A20]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11), c13(r, 12), c14(r, 13), c15(r, 14), c16(r, 15), c17(r, 16), c18(r, 17), c19(r, 18), c20(r, 19))
  }

  implicit def convTuple21[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter, A13: Converter, A14: Converter, A15: Converter, A16: Converter, A17: Converter, A18: Converter, A19: Converter, A20: Converter, A21: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    val c13 = implicitly[Converter[A13]]
    val c14 = implicitly[Converter[A14]]
    val c15 = implicitly[Converter[A15]]
    val c16 = implicitly[Converter[A16]]
    val c17 = implicitly[Converter[A17]]
    val c18 = implicitly[Converter[A18]]
    val c19 = implicitly[Converter[A19]]
    val c20 = implicitly[Converter[A20]]
    val c21 = implicitly[Converter[A21]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11), c13(r, 12), c14(r, 13), c15(r, 14), c16(r, 15), c17(r, 16), c18(r, 17), c19(r, 18), c20(r, 19), c21(r, 20))
  }

  implicit def convTuple22[A1: Converter, A2: Converter, A3: Converter, A4: Converter, A5: Converter, A6: Converter, A7: Converter, A8: Converter, A9: Converter, A10: Converter, A11: Converter, A12: Converter, A13: Converter, A14: Converter, A15: Converter, A16: Converter, A17: Converter, A18: Converter, A19: Converter, A20: Converter, A21: Converter, A22: Converter](r: RowLike, i: Int): (A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22) = {
    val c1 = implicitly[Converter[A1]]
    val c2 = implicitly[Converter[A2]]
    val c3 = implicitly[Converter[A3]]
    val c4 = implicitly[Converter[A4]]
    val c5 = implicitly[Converter[A5]]
    val c6 = implicitly[Converter[A6]]
    val c7 = implicitly[Converter[A7]]
    val c8 = implicitly[Converter[A8]]
    val c9 = implicitly[Converter[A9]]
    val c10 = implicitly[Converter[A10]]
    val c11 = implicitly[Converter[A11]]
    val c12 = implicitly[Converter[A12]]
    val c13 = implicitly[Converter[A13]]
    val c14 = implicitly[Converter[A14]]
    val c15 = implicitly[Converter[A15]]
    val c16 = implicitly[Converter[A16]]
    val c17 = implicitly[Converter[A17]]
    val c18 = implicitly[Converter[A18]]
    val c19 = implicitly[Converter[A19]]
    val c20 = implicitly[Converter[A20]]
    val c21 = implicitly[Converter[A21]]
    val c22 = implicitly[Converter[A22]]
    (c1(r, 0), c2(r, 1), c3(r, 2), c4(r, 3), c5(r, 4), c6(r, 5), c7(r, 6), c8(r, 7), c9(r, 8), c10(r, 9), c11(r, 10), c12(r, 11), c13(r, 12), c14(r, 13), c15(r, 14), c16(r, 15), c17(r, 16), c18(r, 17), c19(r, 18), c20(r, 19), c21(r, 20), c22(r, 21))
  }
}

object CoreTypes extends CoreTypes
