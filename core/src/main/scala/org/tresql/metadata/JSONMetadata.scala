package org.tresql.metadata

import util.parsing.json.JSON
import org.tresql.Metadata
import sys._

//TODO improve json parser so it accepts property names without quotes

case class JSONMetadata(val metadata:Map[String, Table]) extends Metadata {

  override def table(name: String) = metadata(name)
  override def tableOption(name:String) = metadata.get(name)
  override def procedure(name: String) = sys.error("Unsupported method")
  override def procedureOption(name: String) = sys.error("Unsupported method")
}

object JSONMetadata {
  def main(args: Array[String]) = {
    args.length match {
      case 0 => println("usage: <meta data file>")
      case _ => println(fromFile(args(0)))
    }
  }

  def fromFile(file: String) = fromString(scala.io.Source.fromFile(file).mkString)
  def fromString(s: String) = {val t = build(JSON.parseFull(s)); new JSONMetadata(t._2)}

  private def build(md: Option[Any]) = {
    val hm = scala.collection.mutable.HashMap[String, Table]()
    md match {
      case Some(db: Map[String @unchecked, Any @unchecked]) => (db("name").asInstanceOf[String],
              {db("tables").asInstanceOf[List[Map[String, Any]]] foreach {lt =>
              val t = Table(lt); hm += (t.name -> t)}; hm.toMap})
      case x => error("error parsing meta data: " + md)
    }
  }
}
