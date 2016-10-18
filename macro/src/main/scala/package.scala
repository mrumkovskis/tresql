package org

package object tresql extends CoreTypes {

  /**
   *  tresql string interpolator.
   *  NOTE: If variable reference in tresql string ends with ?
   *  i.e. variable is optional and it's value is null, it is filtered out of parameters to be
   *  passed to Query.
   */

  implicit val resources: Resources = Env

  implicit class Tresql(val sc: StringContext) extends AnyVal {
    def tresql(params: Any*)(implicit resources: Resources) = {
      val p = sc.parts
      var optionalVars = Set[Int]()
      Query(p.head + p.tail.zipWithIndex.map(t => {
        if (t._1.trim.startsWith("?")) optionalVars += t._2
        ":_" + t._2 + t._1
      }).mkString, params.zipWithIndex.filterNot(t => t._1 == null && (optionalVars contains t._2))
        .map(t => ("_" + t._2) -> t._1).toMap)
    }
  }

  import scala.reflect.macros.Context
  import scala.language.experimental.macros
  implicit class Tresqlc(val sc: StringContext) extends AnyVal {
    def tresqlc(params: Any*)(implicit resources: Resources): Unit = macro Macros.impl
  }

  object Macros {
    def impl(c: Context)(params: c.Expr[Any]*)(resources: c.Expr[Resources]): c.Expr[Unit] = {
      import c.universe._
      println(s"""Args: $params,
        |env: $resources,
        |context settings: ${c.settings},
        |tree: ${c.macroApplication}""".stripMargin)
      c.Expr[Unit](q"""println("hello world")""")
    }
  }

  implicit def jdbcResultToTresqlResult(jdbcResult: java.sql.ResultSet) = {
    val md = jdbcResult.getMetaData
    new SelectResult(jdbcResult, Vector((1 to md.getColumnCount map {
      i => Column(i, md.getColumnLabel(i), null)
    }): _*), Env(Map(), false), "<not available>", Nil, Env.maxResultSize)
  }
}
