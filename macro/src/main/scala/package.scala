package org

package object tresql extends CoreTypes {

  /**
   *  tresql string interpolator.
   *  NOTE: If variable reference in tresql string ends with ?
   *  i.e. variable is optional and it's value is null, it is filtered out of parameters to be
   *  passed to Query.
   */

  implicit class Tresql(val sc: StringContext) extends AnyVal {
    def tresql(params: Any*)(implicit resources: Resources = Env) = {
      val p = sc.parts
      var optionalVars = Set[Int]()
      Query(p.head + p.tail.zipWithIndex.map(t => {
        if (t._1.trim.startsWith("?")) optionalVars += t._2
        ":_" + t._2 + t._1
      }).mkString, params.zipWithIndex.filterNot(t => t._1 == null && (optionalVars contains t._2))
        .map(t => ("_" + t._2) -> t._1).toMap)
    }
  }

  import scala.language.experimental.macros
  import scala.reflect.macros.blackbox.Context
  implicit class Tresqlc(val sc: StringContext) extends AnyVal {
    def impl(c: Context)(annottees: c.Expr[Any]*)(x: c.Expr[Any]): c.Expr[Any] = {
      import c.universe._
      c.Expr[Unit](q"""println("Hello World")""")
    }

    //def tresqlc(params: Any*)(implicit resources: Resources = Env) = macro impl
  }

  object Macros {
    def impl(c: Context) = {
      import c.universe._
      c.Expr[Unit](q"""println("Hello World")""")
    }

    //def hello(s: String): Unit = macro impl
  }

  implicit def jdbcResultToTresqlResult(jdbcResult: java.sql.ResultSet) = {
    val md = jdbcResult.getMetaData
    new SelectResult(jdbcResult, Vector((1 to md.getColumnCount map {
      i => Column(i, md.getColumnLabel(i), null)
    }): _*), Env(Map(), false), "<not available>", Nil, Env.maxResultSize)
  }
}
