package org.tresql

class QueryParser(override protected val macros: MacroResources = null, cache: Cache = null)
  extends parsing.QueryParsers with parsing.ExpTransformer {

  override def parseExp(expr: String): parsing.Exp = {
    if (cache == null) super.parseExp(expr)
    else cache.get(expr).getOrElse {
      val e = super.parseExp(expr)
      cache.put(expr, e)
      e
    }
  }

  def transformTresql(tresql: String, transformer: Transformer): String =
    this.transformer(transformer)(parseExp(tresql)) tresql

  def extractVariables(exp: String) =
    traverser(variableExtractor)(Nil)(parseExp(exp)).reverse
}
