package org.tresql

object QueryParser extends parsing.QueryParsers with parsing.ExpTransformer {

  def parseExp(expr: String): Any = {
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

  def transformTresql(tresql: String, transformer: PartialFunction[Exp, Exp]): String =
    parseExp(tresql) match {
      case exp: Exp @unchecked => this.transformer(transformer)(exp) tresql
      case _ => tresql
    }

  def extractVariables(exp: String) =
    extractor(variableExtractor)(Nil -> parseExp(exp).asInstanceOf[Exp]).reverse
}
