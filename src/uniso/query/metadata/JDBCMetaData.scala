package uniso.query.metadata

import uniso.query.MetaData
import uniso.query._
import scala.collection.JavaConversions._
import java.sql.{ Connection => C }
import java.sql.ResultSet

//TODO all names perhaps should be stored in upper case?
class JDBCMetaData(private val db: String, private val defaultSchema: String)
    extends MetaData {
    val metaData = new java.util.concurrent.ConcurrentHashMap[String, Table]
    override def dbName = db
    override def table(name: String)(implicit conn:C) = {
        try {
            metaData(name)
        } catch {
            case _: NoSuchElementException => {
                val dmd = conn.getMetaData
                val rs = (if (dmd.storesUpperCaseIdentifiers) name.toUpperCase 
                        else name).split("\\.") match {
                    case Array(t) => dmd.getTables(null,
                            if (dmd.storesUpperCaseIdentifiers) defaultSchema.toUpperCase
                            else defaultSchema, t, null)
                    case Array(s, t) => dmd.getTables(null, s, t, null)
                    case Array(c, s, t) => dmd.getTables(c, s, t, null)
                }
                while (rs.next) {
                    val schema = rs.getString("TABLE_SCHEM")
                    val tableName = rs.getString("TABLE_NAME")
                    val tableType = rs.getString("TABLE_TYPE")
                    val remarks = rs.getString("REMARKS")
                    val mdh = Map("name" -> tableName,
                        "cols" -> cols(dmd.getColumns(null, schema, tableName, null)),
                        "key" -> key(dmd.getPrimaryKeys(null, schema, tableName)),
                        "refs" -> refs(dmd.getImportedKeys(null, schema, tableName)))
                    metaData += (name -> Table(mdh))
                }
                rs.close
                metaData(name)
            }
        }
    }
    def cols(rs: ResultSet) = {
        import scala.collection.mutable.ListBuffer
        val l: ListBuffer[Map[String, String]] = ListBuffer()
        while (rs.next) {
            val name = rs.getString("COLUMN_NAME")
            val typeInt = rs.getInt("DATA_TYPE")
            val typeName = rs.getString("TYPE_NAME")
            val size = rs.getInt("COLUMN_SIZE")
            val decDig = rs.getInt("DECIMAL_DIGITS")
            val nullable = rs.getInt("NULLABLE")
            val comments = rs.getString("REMARKS")
            l += Map("name" -> name, "type" -> typeName)
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
        var trfs: ListBuffer[ListBuffer[String]] = null
        var crfs: ListBuffer[String] = null
        while (rs.next) {
            val pkTable = rs.getString("PKTABLE_NAME");
            val fkColName = rs.getString("FKCOLUMN_NAME");
            val pkColName = rs.getString("PKCOLUMN_NAME");
            val keySeq = rs.getShort("KEY_SEQ");
            val fkName = rs.getString("FK_NAME");
            if (pkTable != prevTable) {
                try {
                    trfs = l(pkTable)("refs").asInstanceOf[ListBuffer[ListBuffer[String]]]
                } catch {
                    case _:NoSuchElementException => {
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
            crfs += fkColName
        }
        rs.close
        (l.values.map {t => Map("table" -> t("table"), 
                "refs" -> (t("refs").asInstanceOf[ListBuffer[ListBuffer[String]]] map
                        {_.toList}).toList)}).toList
    }
}

object JDBCMetaData extends ((String, String) => JDBCMetaData){
    
    def apply(db: String, defaultSchema: String) = {
        new JDBCMetaData(db, defaultSchema)
    }

    def main(args: Array[String]) {
        args.length < 6 match {
            case true => println("usage: <driver> <dburl> <user> <password> <default schema> <table>")
            case false => {
                val c = Conn(args(0), args(1), args(2), args(3))()
                try {
                    val md = JDBCMetaData(args(1), args(4))
                    println(md.table(args(5))(c))
                } finally {
                    c.close
                }
            }
        }
    }

}