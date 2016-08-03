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

  implicit def jdbcResultToTresqlResult(jdbcResult: java.sql.ResultSet) = {
    val md = jdbcResult.getMetaData
    new SelectResult(jdbcResult, Vector((1 to md.getColumnCount map {
      i => Column(i, md.getColumnLabel(i), null)
    }): _*), Env(Map(), false), "<not available>", Nil, Env.maxResultSize)
  }
}
