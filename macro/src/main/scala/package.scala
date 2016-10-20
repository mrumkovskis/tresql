package org

package object tresql extends CoreTypes {

  /**
   *  tresql string interpolator.
   *  NOTE: If variable reference in tresql string ends with ?
   *  i.e. variable is optional and it's value is null, it is filtered out of parameters to be
   *  passed to Query.
   */

  implicit val resources: Resources = Env

  //whitebox context is important (not blackbox) since return value of tresql function
  //must return type actually returned from Macro.impl i.e not CompiledResult[RowLike]
  import scala.reflect.macros.whitebox.Context
  import scala.language.experimental.macros
  implicit class Tresql(val sc: StringContext) extends AnyVal {
    def tresql(params: Any*)(implicit resources: Resources): Result = macro Macros.impl
  }

  object Macros {
    def impl(c: Context)(params: c.Expr[Any]*)(resources: c.Expr[Resources]): c.Expr[Result] = {
      import c.universe._
      val q"$surroundingTree" = c.macroApplication
      val q"org.tresql.`package`.Tresql(scala.StringContext.apply(..$parts)).tresql(..$pars)($res)" =
        c.macroApplication
      val tresqlString = parts.map { case Literal(Constant(x)) => x } match {
        case l => l.head + l.tail.zipWithIndex.map(t => ":_" + t._2 + t._1).mkString //replace placeholders with variable defs
      }
      println(s"Macro compiler settings: ${c.settings}")
      println(s"Compiling: $tresqlString")
      //QueryCompiler.compile(tresqlString)
      val tree = q"""
        var optionalVars = Set[Int]()
        Query(${parts.head} + List[String](..${parts.tail}).zipWithIndex.map { t =>
          if (t._1.trim.startsWith("?")) optionalVars += t._2
          ":_" + t._2 + t._1
        }.mkString, List[Any](..$params)
          .zipWithIndex
          .filterNot(t => t._1 == null && (optionalVars contains t._2))
          .map(t => ("_" + t._2) -> t._1).toMap)($res)"""
      c.Expr(tree)
    }
  }

  implicit def jdbcResultToTresqlResult(jdbcResult: java.sql.ResultSet) = {
    val md = jdbcResult.getMetaData
    new SelectResult(jdbcResult, Vector((1 to md.getColumnCount map {
      i => Column(i, md.getColumnLabel(i), null)
    }): _*), Env(Map(), false), "<not available>", Nil, Env.maxResultSize)
  }
}
