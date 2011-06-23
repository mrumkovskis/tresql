package uniso.query;
import java.util.{ ArrayList, List => JList, Map => JMap }
import scala.collection.JavaConversions._

object JavaQuery {
  /* not tested
  def ids(expr: String, params: JMap[String, Any]): JList[Long] = {
    resultToIds(Query(expr, params))
  }
  */

  def ids(expr: String, params: JList[Any]): JList[Long] = {
    val parsSeq: Seq[Any] = params
    resultToIds(Query(expr, parsSeq.toList))
  }

  private def resultToIds(result: Any): JList[Long] = {
    var ids = new ArrayList[Long]()
    result match {
      case r: Result =>
        r.foreach { i =>
          ids.add(i.asInstanceOf[Long])
        }
      case r: Iterable[_] =>
        r.foreach { i =>
          ids.add(i.asInstanceOf[Long])
        }
    }
    ids
  }
}
