package uniso.query.test

import org.scalatest.Suite
import java.sql._

class QueryTest extends Suite {
  Class.forName("org.hsqldb.jdbc.JDBCDriver")
  val conn = DriverManager.getConnection("jdbc:hsqldb:mem:.")
  new scala.io.BufferedSource(getClass.getResourceAsStream("/db.sql")).mkString.split(";").foreach {
    sql => val st = conn.createStatement; st.execute(sql); st.close
  }
  
  def testEcho {
    assert(1 === 1)
  }

}