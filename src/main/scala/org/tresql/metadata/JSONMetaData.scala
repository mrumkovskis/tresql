package org.tresql.metadata

import util.parsing.json.JSON
import org.tresql.MetaData
import sys._

//TODO improve json parser so it accepts property names without quotes

case class JSONMetaData(val metaData:Map[String, Table]) extends MetaData {
  
  def table(name: String) = metaData(name)
  def tableOption(name:String) = metaData.get(name)
  def procedure(name: String) = sys.error("Unsupported method")
  def procedureOption(name: String) = sys.error("Unsupported method")
}

object JSONMetaData {
  def main(args: Array[String]) {
    args.length match {
      case 0 => println("usage: <meta data file>")
      case _ => println(fromFile(args(0)))
    }
  }

  def fromFile(file: String) = fromString(scala.io.Source.fromFile(file).mkString)
  def fromString(s: String) = {val t = build(JSON.parseFull(s)); new JSONMetaData(t._2)}

  private def build(md: Option[Any]) = {
    val hm = scala.collection.mutable.HashMap[String, Table]()
    md match {
      case Some(db: Map[String, Any]) => (db("name").asInstanceOf[String], 
              {db("tables").asInstanceOf[List[Map[String, Any]]] foreach {lt => 
              val t = Table(lt); hm += (t.name -> t)}; hm.toMap})
      case x => error("error parsing meta data: " + md)
    }
  }

}