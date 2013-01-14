package org

package object tresql {

  implicit def jdbcResultToTresqlResult(jdbcResult: java.sql.ResultSet) = {
    val md = jdbcResult.getMetaData
	new Result(jdbcResult, Vector((1 to md.getColumnCount map {
	  i=> Column(i, md.getColumnLabel(i), null)
	}) : _*), Env(Map(), false))
  }

}