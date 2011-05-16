package code
package snippet

import QueryServer.{ P_, Plen }
import net.liftweb.http.js.JsCmd._
import net.liftweb.http.js._
import net.liftweb.http._
import net.liftweb.util.Helpers._
import net.liftweb._
import scala.collection.mutable.Map

object AjaxQuery {
  def render = {
    var queryString = ""
    var expectedPars = List("p_1", "p_2", "p_3")
    val pars = Map("p_1" -> "", "p_2" -> "", "p_3" -> "")
    var resultString = ""
    var tableElement: IdMemoizeTransform = null
    var doQuery = false

    def process(): JsCmd = {
      try {
        expectedPars = QueryServer.bindVariables(queryString).map(P_ + _)
        if (doQuery) {
          S.notice("QUERY: " + queryString)
          val queryPars =
            (for (p <- expectedPars) yield {
              if (!(pars contains p)) pars(p) = ""
              p.substring(Plen) -> pars(p)
            }).toMap
          println("PARS: " + queryPars)
          S.notice("PARS: " + queryPars)
          resultString = QueryServer.json(queryString, queryPars)
          S.notice("RESULT: " + resultString)
        }
      } catch {
        case e: Exception => e.printStackTrace; S.notice("ERROR: " + e.toString)
      }
      SHtml.ajaxInvoke(tableElement.setHtml _)._2.cmd
    }

    def setDoQuery(b: Boolean) = { doQuery = b; JsCmds.Noop }

    "#result *" #> resultString &
      "name=query" #> SHtml.textarea(queryString, queryString = _) &
      "#params" #> SHtml.idMemoize(table =>
        "table" #>
          <table>{
            tableElement = table
            expectedPars.map(p =>
              <tr>
                <td>{ p.substring(Plen) }</td>
                <td>{
                  SHtml.text(if (pars.contains(p)) pars(p) else "", pars(p) = _)
                }</td>
              </tr>)
          }</table>) &
      "#refresh-pars [onclick]" #> SHtml.ajaxInvoke(() => setDoQuery(false)) &
      "#execute-query [onclick]" #> SHtml.ajaxInvoke(() => setDoQuery(true)) &
      "#hideme" #> S.formGroup(1000) { SHtml.hidden(process) }
  }
}
