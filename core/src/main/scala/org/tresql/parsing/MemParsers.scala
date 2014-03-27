package org.tresql.parsing

trait MemParsers extends scala.util.parsing.combinator.Parsers {
  protected val intermediateResults = new ThreadLocal[scala.collection.mutable.Map[(String, Int), ParseResult[Any]]]
  intermediateResults.set(scala.collection.mutable.Map())

  class MemParser[+T](underlying: Parser[T]) extends Parser[T] {
    def apply(in: Input) = intermediateResults.get.get(underlying.toString -> in.offset)
      .map(_.asInstanceOf[ParseResult[T]])
      .getOrElse {
        val o = in.offset
        val r = underlying.apply(in)
        intermediateResults.get += ((underlying.toString -> o) -> r)
        r
      }
  }

  implicit def parser2MemParser[T](parser: Parser[T]) = new MemParser(parser)
}