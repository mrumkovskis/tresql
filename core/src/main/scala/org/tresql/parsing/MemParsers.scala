package org.tresql.parsing

trait MemParsers extends scala.util.parsing.combinator.Parsers {
  protected val intermediateResults = new ThreadLocal[scala.collection.mutable.Map[(String, Int), ParseResult[Any]]] {
    override def initialValue = scala.collection.mutable.HashMap()
  }

  class MemParser[+T](underlying: Parser[T]) extends Parser[T] {
    def apply(in: Input) = intermediateResults.get.get(underlying.toString -> in.offset)
      .map(_.asInstanceOf[ParseResult[T]]).getOrElse {
        val r = underlying(in)
        intermediateResults.get += ((underlying.toString -> in.offset) -> r)
        r
      }
  }

  implicit def parser2MemParser[T](parser: Parser[T]) = new MemParser(parser)
}