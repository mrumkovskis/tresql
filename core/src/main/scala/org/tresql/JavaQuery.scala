package org.tresql;
import java.util.{ ArrayList, List => JList, Map => JMap }
import scala.collection.JavaConversions._

object JavaQuery {

  def ids(expr: String, params: JMap[String, Any]): JList[java.lang.Long] = {
    resultToIds(Query(expr, asScalaMap(params).toMap))
  }

  def ids(expr: String, params: JList[Any]): JList[java.lang.Long] = {
    val parsSeq: Seq[Any] = params
    resultToIds(Query(expr, parsSeq.toList))
  }

  private def countExpr(expr: String) = "/(" + expr + ") {count(*)}"

  def count(expr: String, params: JMap[String, Any]): java.lang.Long = {
    resultToCount(Query(countExpr(expr), asScalaMap(params).toMap))
  }

  def count(expr: String, params: JList[Any]): java.lang.Long = {
    val parsSeq: Seq[Any] = params
    resultToCount(Query(countExpr(expr), parsSeq.toList))
  }

  private def resultToIds(result: Any): JList[java.lang.Long] = {
    var ids = new ArrayList[java.lang.Long]()
    result.asInstanceOf[Result].foreach { r =>
      ids.add(r(0).asInstanceOf[java.lang.Long])
    }
    ids
  }

  private def resultToCount(result: Any): Long = {
    val res: Result = result.asInstanceOf[Result]
    val count =
      if (!res.hasNext()) {
        throw new IllegalStateException("resultToCount() expects 1 row")
      } else {
        res.next()
        res(0).asInstanceOf[java.lang.Number].longValue
      }
    if (res.hasNext()) {
      throw new IllegalStateException("resultToCount() expects 1 row")
    } else {
      count
    }
  }
}
