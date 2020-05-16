package org.tresql

class QueryParser(protected val resources: Resources)
  extends parsing.QueryParsers with parsing.ExpTransformer {

  override def parseExp(expr: String): parsing.Exp = {
    Env.cache.flatMap(_.get(expr)).getOrElse {
      val e = super.parseExp(expr)
      Env.cache.map(_.put(expr, e))
      e
    }
  }

  def transformTresql(tresql: String, transformer: Transformer): String =
    this.transformer(transformer)(parseExp(tresql)) tresql

  def extractVariables(exp: String) =
    traverser(variableExtractor)(Nil)(parseExp(exp)).reverse
}
