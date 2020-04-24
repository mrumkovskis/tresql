package org.tresql

object QueryParser extends parsing.QueryParsers with parsing.ExpTransformer {

  override def parseExp(expr: String): Exp = {
    Env.cache.flatMap(_.get(expr)).getOrElse {
      try {
        intermediateResults.get.clear
        val e = phrase(exprList)(new scala.util.parsing.input.CharSequenceReader(expr)) match {
          case Success(r, _) => r
          case x => sys.error(x.toString)
        }
        Env.cache.map(_.put(expr, e))
        e
      } finally intermediateResults.get.clear
    }
  }

  def transformTresql(tresql: String, transformer: Transformer): String =
    this.transformer(transformer)(parseExp(tresql)) tresql

  def extractVariables(exp: String) =
    traverser(variableExtractor)(Nil)(parseExp(exp)).reverse
}
