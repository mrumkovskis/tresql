package org.tresql

object QueryParser extends parsing.QueryParsers with parsing.ExpTransformer {

  override def parseExp(expr: String)(implicit resources: Resources): parsing.Exp = {
    Env.cache.flatMap(_.get(expr)).getOrElse {
      val e = super.parseExp(expr)
      Env.cache.map(_.put(expr, e))
      e
    }
  }

  def transformTresql(tresql: String, transformer: Transformer)(implicit resources: Resources): String =
    this.transformer(transformer)(parseExp(tresql)) tresql

  def extractVariables(exp: String)(implicit resources: Resources) =
    traverser(variableExtractor)(Nil)(parseExp(exp)).reverse
}
