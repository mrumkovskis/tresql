package org.tresql.metadata

import org.tresql.Metadata
import org.tresql._
import scala.collection.JavaConverters._
import java.sql.{ Connection => C }
import java.sql.ResultSet
import java.sql.DatabaseMetaData
import java.util.concurrent.ConcurrentHashMap

//TODO all names perhaps should be stored in upper case?
//This class is thread safe i.e. instance can be used across multiple threads
trait JDBCMetadata extends Metadata {

  private val tableCache = new ConcurrentHashMap[String, Table]
  private val procedureCache = new ConcurrentHashMap[String, Procedure[_]]

  def conn: java.sql.Connection
  def defaultSchema: String = null
  def publicSchemaName = "PUBLIC"

  override def table(name: String) = tableOption(name)
    .getOrElse(sys.error(s"Table not found: $name"))
  override def tableOption(name: String) = Option(tableCache.get(name)).orElse {
    if (conn == null) throw new NullPointerException(
      """Connection not found in environment. Check if "Env.conn = conn" (in this case statement execution must be done in the same thread) or "Env.sharedConn = conn" is called.""")
    val dmd = conn.getMetaData
    val rs = (if (dmd.storesUpperCaseIdentifiers) name.toUpperCase
    else name).split("\\.") match {
      case Array(t) => dmd.getTables(null,
        if (dmd.storesUpperCaseIdentifiers &&
          defaultSchema != null) defaultSchema.toUpperCase
        else defaultSchema, t, null)
      case Array(s, t) => dmd.getTables(null, s, t, null)
      case Array(c, s, t) => dmd.getTables(c, s, t, null)
      case x => sys.error(s"Unexpected table name: '$name'")
    }
    var m = Set[(String, String)]()
    while (rs.next) {
      val schema = rs.getString("TABLE_SCHEM")
      val tableName = rs.getString("TABLE_NAME")
      val qualifiedTableName =
        s"${if (schema == null || schema.equalsIgnoreCase(publicSchemaName)) "" else schema + "."}$tableName"
      m += Option(schema).getOrElse("<null>") -> tableName
      if (m.size > 1) {
        tableCache remove name
        rs.close
        throw new RuntimeException(
          "Ambiguous table name: " + name + "." + " Both " +
            m.map((t) => t._1 + "." + t._2).mkString(" and ") + " match")
      }
      val tableType = rs.getString("TABLE_TYPE")
      val remarks = rs.getString("REMARKS")
      val mdh = Map("name" -> qualifiedTableName, "comments" -> remarks,
        "cols" -> cols(dmd.getColumns(null, schema, tableName, null)),
        "key" -> key(dmd.getPrimaryKeys(null, schema, tableName)),
        "refs" -> refs(dmd.getImportedKeys(null, schema, tableName)))
      tableCache.putIfAbsent(name, Table(mdh))
    }
    rs.close
    Option(tableCache.get(name))
  }

  override def procedure(name: String): Procedure[_] = procedureOption(name)
    .getOrElse(sys.error(s"Procedure not found: $name"))
  override def procedureOption(name: String) = Option(procedureCache.get(name)).orElse {
    if (conn == null) throw new NullPointerException(
      """Connection not found in environment. Check if "Env.conn = conn" (in this case statement execution must be done in the same thread) or "Env.sharedConn = conn" is called.""")
    val dmd = conn.getMetaData
    val rs = (if (dmd.storesUpperCaseIdentifiers) name.toUpperCase
    else name).split("\\.") match {
      case Array(p) => dmd.getProcedures(null,
        if (dmd.storesUpperCaseIdentifiers &&
          defaultSchema != null) defaultSchema.toUpperCase
        else defaultSchema, p)
      case Array(s, p) => dmd.getProcedures(null, s, p)
      case Array(c, s, p) => dmd.getProcedures(c, s, p)
      case x => sys.error(s"Unexpected procedure name: '$name'")
    }
    var m = Set[(String, String)]()
    while (rs.next) {
      val schema = rs.getString("PROCEDURE_SCHEM")
      val procedureName = rs.getString("PROCEDURE_NAME")
      m += Option(schema).getOrElse("<null>") -> procedureName
      if (m.size > 1) {
        procedureCache remove name
        rs.close
        throw new RuntimeException(
          "Ambiguous procedure name: " + name + "." + " Both " +
            m.map((t) => t._1 + "." + t._2).mkString(" and ") + " match")
      }
      val procedureType = rs.getInt("PROCEDURE_TYPE")
      val remarks = rs.getString("REMARKS")
      var pars = List[Par[_]]()
      val parsRs = dmd.getProcedureColumns(null, schema, procedureName, null)
      import parsRs._
      while(next) {
        pars = Par(getString("COLUMN_NAME").toLowerCase,
            getString("REMARKS"),
            getInt("COLUMN_TYPE"),
            getInt("DATA_TYPE"),
            getString("TYPE_NAME"),
            sql_scala_type_map(getInt("DATA_TYPE")))::pars
      }
      parsRs.close
      val returnPar = pars.filter(_.parType == DatabaseMetaData.procedureColumnReturn) match {
        case par::Nil => (par.sqlType, par.typeName, par.scalaType)
        case _ => (-1, null, null)
      }
      procedureCache.put(name, Procedure(procedureName.toLowerCase, remarks, procedureType,
          pars.reverse, returnPar._1, returnPar._2, returnPar._3))
    }
    rs.close
    Option(procedureCache.get(name))
  }

  def cols(rs: ResultSet) = {
    import scala.collection.mutable.ListBuffer
    val l: ListBuffer[Map[String, Any]] = ListBuffer()
    while (rs.next) {
      l += Map(
        "name" -> rs.getString("COLUMN_NAME"),
        "sql-type" -> rs.getInt("DATA_TYPE"),
        "scala-type" -> sql_scala_type_map(rs.getInt("DATA_TYPE")),
        "type-name" -> rs.getString("TYPE_NAME"),
        "size" -> rs.getInt("COLUMN_SIZE"),
        "decimalDigits" -> rs.getInt("DECIMAL_DIGITS"),
        "nullable" -> (rs.getInt("NULLABLE") == DatabaseMetaData.columnNullable),
        "comments" -> rs.getString("REMARKS"))
    }
    rs.close
    l.toList
  }
  def key(rs: ResultSet) = {
    var cols: List[String] = Nil
    while (rs.next) {
      val colName = rs.getString("COLUMN_NAME")
      val keySeq = rs.getShort("KEY_SEQ")
      val pkName = rs.getString("PK_NAME")
      cols = cols :+ colName
    }
    rs.close
    cols
  }
  def refs(rs: ResultSet) = {
    import scala.collection.mutable.ListBuffer
    var prevTable = null
    val l: scala.collection.mutable.Map[String, scala.collection.mutable.Map[String, Any]] =
      scala.collection.mutable.Map()
    var trfsm: scala.collection.mutable.Map[String, Any] = null
    var trfs: ListBuffer[ListBuffer[(String, String)]] = null
    var crfs: ListBuffer[(String, String)] = null
    while (rs.next) {
      val pkSchema = rs.getString("PKTABLE_SCHEM")
      val pkTable =
        (if (pkSchema == null || pkSchema.equalsIgnoreCase(publicSchemaName)) "" else pkSchema + ".") +
          rs.getString("PKTABLE_NAME");
      val fkColName = rs.getString("FKCOLUMN_NAME");
      val pkColName = rs.getString("PKCOLUMN_NAME");
      val keySeq = rs.getShort("KEY_SEQ");
      val fkName = rs.getString("FK_NAME");
      if (pkTable != prevTable) {
        try {
          trfs = l(pkTable)("refs").asInstanceOf[ListBuffer[ListBuffer[(String, String)]]]
        } catch {
          case _: NoSuchElementException => {
            trfs = ListBuffer()
            trfsm = scala.collection.mutable.Map("table" -> pkTable, "refs" -> trfs)
            l += (pkTable -> trfsm)
          }
        }
      }
      if (keySeq == 1) {
        crfs = ListBuffer()
        trfs += crfs
      }
      crfs += (fkColName -> pkColName)
    }
    rs.close
    l.values.map { t =>
      Map("table" -> t("table"),
        "refs" -> (t("refs").asInstanceOf[ListBuffer[ListBuffer[String]]] map {
          _.toList
        }).toList)
    }.toList
  }
}

object JDBCMetadata {
  def apply(_conn: java.sql.Connection = null, defaultSch: String = null) = {
    new JDBCMetadata {
      override def conn = _conn //USING conn HANGS UP JVM in eternal loop
      override def defaultSchema = defaultSch
    }
  }
}
