package org.tresql.parsing

trait MemParsers extends scala.util.parsing.combinator.Parsers {
  private val intermediateResults = new ThreadLocal[scala.collection.mutable.Map[(String, Int), ParseResult[_]]] {
    override def initialValue: scala.collection.mutable.Map[(String, Int), ParseResult[_]] =
      scala.collection.mutable.HashMap()
  }

  class MemParser[+T](underlying: Parser[T]) extends Parser[T] {
    def apply(in: Input) = intermediateResults.get.get(underlying.toString -> in.offset)
      .map(_.asInstanceOf[ParseResult[T]]).getOrElse {
        val r = underlying(in)
        intermediateResults.get += ((underlying.toString -> in.offset) -> r)
        r
      }
  }

  override def phrase[T](p: Parser[T]): Parser[T] = {
    val phrp = super.phrase(p)
    new Parser[T] {
      def apply(in: Input) = {
        //each phrase (possibly nested, e.g. macro_ interpolator) gets its own memo, outer memo is restored
        val outer = intermediateResults.get
        intermediateResults.set(scala.collection.mutable.HashMap())
        try phrp(in)
        finally intermediateResults.set(outer)
      }
    }
  }

  implicit def parser2MemParser[T](parser: Parser[T]): MemParser[T] = new MemParser(parser)
}