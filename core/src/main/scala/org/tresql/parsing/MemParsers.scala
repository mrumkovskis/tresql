package org.tresql.parsing

trait MemParsers extends scala.util.parsing.combinator.Parsers {
  val intermediateResults = new scala.util.DynamicVariable(scala.collection.mutable.Map[(String, Int), ParseResult[Any]]())

  class MemParser[+T](underlying: Parser[T]) extends Parser[T] {
    def apply(in: Input) = intermediateResults.value.get(underlying.toString -> in.offset)
      .map(_.asInstanceOf[ParseResult[T]])
      .getOrElse {
        val o = in.offset
        val r = underlying.apply(in)
        intermediateResults.value += ((underlying.toString -> o) -> r)
        r
      }
  }

  implicit def parser2MemParser[T](parser: Parser[T]) = new MemParser(parser)
}