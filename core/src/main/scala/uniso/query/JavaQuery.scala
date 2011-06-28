package uniso.query;
import java.util.{ ArrayList, List => JList, Map => JMap }
import scala.collection.JavaConversions._

object JavaQuery {

  def ids(expr: String, params: JMap[String, Any]): JList[Long] = {
    resultToIds(Query(expr, asScalaMap(params).toMap))
  }

  def ids(expr: String, params: JList[Any]): JList[Long] = {
    val parsSeq: Seq[Any] = params
    resultToIds(Query(expr, parsSeq.toList))
  }

  private def resultToIds(result: Any): JList[Long] = {
    var ids = new ArrayList[Long]()
    result.asInstanceOf[Result].foreach { r =>
      ids.add(r(0).asInstanceOf[Long])
    }
    ids
  }
}
