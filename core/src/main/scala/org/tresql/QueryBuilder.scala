package org.tresql

import sys._
import QueryParser._

class QueryBuilder private (val env: Env, private val queryDepth: Int,
  private var bindIdx: Int) extends EnvProvider {

  val ROOT_CTX = "ROOT"
  val QUERY_CTX = "QUERY"
  val TABLE_CTX = "TABLE"
  val JOIN_CTX = "JOIN"
  val WHERE_CTX = "WHERE"
  val COL_CTX = "COL"
  val ORD_CTX = "ORD"
  val GROUP_CTX = "GROUP"
  val HAVING_CTX = "HAVING"
  val VALUES_CTX = "VALUES"
  val LIMIT_CTX = "LIMIT"
  val FUN_CTX = "FUNCTION"
  val EXTERNAL_FUN_CTX = "EXTERNAL_FUN_CTX"

  private def this(env: Env) = this(env, 0, 0)

  //used for non flat updates, i.e. hierarhical object.
  private val childUpdates = scala.collection.mutable.ListBuffer[(Expr, String)]()

  //context stack as buildInternal method is called
  private var ctxStack = List[String]()
  //bind variables for jdbc prepared statement 
  private val _bindVariables = scala.collection.mutable.ListBuffer[Expr]()
  private lazy val bindVariables = _bindVariables.toList

  //used internally while building expression
  private var separateQueryFlag = false
  //indicate * in column
  private var allCols = false
  //indicate table.* in column clause
  private var identAll = false
  //indicate presence of hidden columns due to external function call
  private var externalFunction = false

  case class ConstExpr(val value: Any) extends BaseExpr {
    override def apply() = value
    def defaultSQL = value match {
      case v: Number => v.toString
      case v: String => "'" + v + "'"
      case v: Boolean => v.toString
      case null => "null"
    }
  }

  case class AllExpr() extends PrimitiveExpr {
    def defaultSQL = "*"
  }
  case class IdentAllExpr(val name: List[String]) extends PrimitiveExpr {
    QueryBuilder.this.identAll = true
    def defaultSQL = name.mkString(".") + ".*"
  }

  case class VarExpr(val name: String, val opt: Boolean) extends BaseExpr {
    override def apply() = env.get(name) getOrElse (error("Bind variable with name " + name + " not found."))
    var binded = false
    def defaultSQL = {
      if (!binded) QueryBuilder.this._bindVariables += this
      val s = (if (!env.reusableExpr && (env contains name)) {
        env(name) match {
          case l: scala.collection.Traversable[_] =>
            if (l.size > 0) "?," * (l size) dropRight 1 else {
              if (!binded) QueryBuilder.this._bindVariables.trimEnd(1)
              //return null for empty collection (not to fail in 'in' operator)
              "null"
            }
          case a: Array[_] => if (a.length > 0) "?," * (a length) dropRight 1 else {
            if (!binded) QueryBuilder.this._bindVariables.trimEnd(1)
            //return null for empty array (not to fail in 'in' operator)
            "null"
          }
          case _ => "?"
        }
      } else "?")
      binded = true
      s
    }
    override def toString = if (env contains name) name + " = " + env(name) else name
  }

  case class IdExpr(val seqName: String) extends BaseExpr {
    override def apply() = {
      env.tableOption(seqName).map(_.key.cols).filter(_.size == 1).map(_(0))
        .filter(env.contains(_)).flatMap(key => Option(env(key))).map { id =>
          //if primary key is set as an environment variable use it instead of sequence
          env.currId(seqName, id)
          id
        } getOrElse env.nextId(seqName)
    }
    var binded = false
    def defaultSQL = {
      if (!binded) { QueryBuilder.this._bindVariables += this; binded = true }
      "?"
    }
    override def toString = "#" + seqName
  }

  case class IdRefExpr(val seqName: String) extends BaseExpr {
    override def apply() = env.currId(seqName)
    var binded = false
    def defaultSQL = {
      if (!binded) { QueryBuilder.this._bindVariables += this; binded = true }
      "?"
    }
    override def toString = ":#" + seqName
  }

  case class ResExpr(val nr: Int, val col: Any) extends PrimitiveExpr {
    override def apply() = env(nr) match {
      case null => error("Ancestor result with number " + nr + " not found for expression " + this)
      case r => col match {
        case c: List[_] => r(c.mkString("."))
        case c: String => r(c)
        case c: Int if (c > 0) => r(c - 1)
        case c: Int => error("column index in result expression must be greater than 0. Is: " + c)
      }
    }
    var binded = false
    def defaultSQL = {
      if (!binded) { QueryBuilder.this._bindVariables += this; binded = true }
      "?"
    }
    override def toString = nr + "(" + col + ")"
  }

  case class AssignExpr(val variable: String, val value: Expr) extends BaseExpr {
    //add variable to environment so that variable is found when referenced in further expressions
    env(variable) = null
    override def apply() = {
      env(variable) = value()
      env(variable)
    }
    def defaultSQL = error("Cannot construct sql statement from assignment expression")
    override def toString = variable + " = " + value
  }

  case class UnExpr(val op: String, val operand: Expr) extends BaseExpr {
    override def apply() = op match {
      case "-" => -operand().asInstanceOf[Number]
      case "!" => !operand().asInstanceOf[Boolean]
      case "|" => operand()
      case _ => error("unknown unary operation " + op)
    }
    def defaultSQL = op match {
      case "-" => "-" + operand.sql
      case "!" =>
        (if (operand.exprType == classOf[SelectExpr]) "not exists " else "not ") +
          operand.sql
      case _ => error("unknown unary operation " + op)
    }
    override def exprType: Class[_] = if ("-" == op) operand.exprType else classOf[ConstExpr]
  }

  case class InExpr(val lop: Expr, val rop: List[Expr], val not: Boolean) extends BaseExpr {
    override def apply = if (not) lop notIn rop else lop in rop
    def defaultSQL = lop.sql + (if (not) " not" else "") + rop.map(_.sql).mkString(" in(", ", ", ")")
    override def exprType = classOf[ConstExpr]
  }

  case class BinExpr(val op: String, val lop: Expr, val rop: Expr) extends BaseExpr {
    override def apply() = {
      def selCols(ex: Expr): List[QueryBuilder#ColExpr] = {
        ex match {
          case e: SelectExpr => e.cols
          case e: BinExpr => selCols(e.lop)
          case e: BracesExpr => selCols(e.expr)
        }
      }
      op match {
        case "*" => lop * rop
        case "/" => lop / rop
        case "||" => lop + rop
        case "&&" => org.tresql.Query.sel(sql, selCols(lop),
          QueryBuilder.this.bindVariables, env, QueryBuilder.this.allCols,
          QueryBuilder.this.identAll, QueryBuilder.this.externalFunction)
        case "++" => org.tresql.Query.sel(sql, selCols(lop),
          QueryBuilder.this.bindVariables, env, QueryBuilder.this.allCols,
          QueryBuilder.this.identAll, QueryBuilder.this.externalFunction)
        case "+" => if (exprType == classOf[SelectExpr])
          org.tresql.Query.sel(sql, selCols(lop), QueryBuilder.this.bindVariables, env,
            QueryBuilder.this.allCols, QueryBuilder.this.identAll, QueryBuilder.this.externalFunction)
        else lop + rop
        case "-" => if (exprType == classOf[SelectExpr])
          org.tresql.Query.sel(sql, selCols(lop), QueryBuilder.this.bindVariables, env,
            QueryBuilder.this.allCols, QueryBuilder.this.identAll, QueryBuilder.this.externalFunction)
        else lop - rop
        case "=" => lop == rop
        case "!=" => lop != rop
        case "<" => lop < rop
        case ">" => lop > rop
        case "<=" => lop <= rop
        case ">=" => lop >= rop
        case "&" => lop & rop
        case "|" => lop | rop
        case _ => error("unknown operation " + op)
      }
    }
    def defaultSQL = op match {
      case "*" => lop.sql + " * " + rop.sql
      case "/" => lop.sql + " / " + rop.sql
      case "||" => lop.sql + " || " + rop.sql
      case "++" => lop.sql + " union all " + rop.sql
      case "&&" => lop.sql + " intersect " + rop.sql
      case "+" => lop.sql + (if (exprType == classOf[SelectExpr]) " union " else " + ") + rop.sql
      case "-" => lop.sql + (if (exprType == classOf[SelectExpr]) " except " else " - ") + rop.sql
      case "=" => rop match {
        case ConstExpr(null) => lop.sql + " is " + rop.sql
        case _: ArrExpr => lop.sql + " in " + rop.sql
        case _ => lop.sql + " = " + rop.sql
      }
      case "!=" => rop match {
        case ConstExpr(null) => lop.sql + " is not " + rop.sql
        case _: ArrExpr => lop.sql + " not in " + rop.sql
        case _ => lop.sql + " != " + rop.sql
      }
      case "<" => lop.sql + " < " + rop.sql
      case ">" => lop.sql + " > " + rop.sql
      case "<=" => lop.sql + " <= " + rop.sql
      case ">=" => lop.sql + " >= " + rop.sql
      case "&" => (if (lop.exprType == classOf[SelectExpr]) "exists " else "") +
        lop.sql + " and " + (if (rop.exprType == classOf[SelectExpr]) "exists " else "") +
        rop.sql
      case "|" => (if (lop.exprType == classOf[SelectExpr]) "exists " else "") + lop.sql +
        " or " + (if (rop.exprType == classOf[SelectExpr]) "exists " else "") + rop.sql
      case "~~" => "upper(" + lop.sql + ") like upper(" + rop.sql + ")"
      case "!~~" => "upper(" + lop.sql + ") not like upper(" + rop.sql + ")"
      case "~" => lop.sql + " like " + rop.sql
      case "!~" => lop.sql + " not like " + rop.sql
      case s @ ("in" | "!in") => lop.sql + (if (s.startsWith("!")) " not" else "") + " in " +
        (rop match {
          case _: BracesExpr | _: ArrExpr => rop.sql
          case _ => "(" + rop.sql + ")"
        })
      case _ => error("unknown operation " + op)
    }
    override def exprType: Class[_] = if (List("&&", "++", "+", "-", "*", "/") exists (_ == op)) {
      if (lop.exprType == rop.exprType) lop.exprType else super.exprType
    } else classOf[ConstExpr]
  }

  case class FunExpr(name: String, params: List[Expr]) extends BaseExpr {
    override def apply() = {
      val p = params map (_())
      val ts = p map {
        case null => classOf[Any]
        case x => x.getClass
      }
      if (Env.isDefined(name)) {
        try {
          Env log ts.mkString("Trying to call locally defined function: " + name + "(", ", ", ")")
          env.functions.map(f => f.getClass.getMethod(name, ts: _*).invoke(
            f, p.asInstanceOf[List[Object]]: _*)).get
        } catch {
          case ex: NoSuchMethodException => {
            env.functions.flatMap(f => f.getClass.getMethods.filter(m =>
              m.getName == name && (m.getParameterTypes match {
                case Array(par) => par.isInstance(p)
                case _ => false
              })).headOption.map(_.invoke(f, List(p).asInstanceOf[List[Object]]: _*)).orElse(Some(
              org.tresql.Query.call("{call " + sql + "}", QueryBuilder.this.bindVariables, env)))).get
          }
        }
      } else org.tresql.Query.call("{call " + sql + "}", QueryBuilder.this.bindVariables, env)
    }
    def defaultSQL = name + (params map (_.sql)).mkString("(", ",", ")")
    override def toString = name + (params map (_.toString)).mkString("(", ",", ")")
  }

  case class ExternalFunExpr(name: String, params: List[Expr],
    method: java.lang.reflect.Method) extends BaseExpr {
    override def apply() = {
      val p = params map (_())
      if (method.getParameterTypes.size != params.size) error("Wrong parameter count for method "
        + method + " " + method.getParameterTypes.size + " != " + params.size)
      Env.functions.map(method.invoke(_, p.asInstanceOf[List[Object]]: _*))
        .getOrElse("Functions not defined in Env!")
    }
    def defaultSQL = error("Method not implemented for external function")
    override def toString = name + (params map (_.toString)).mkString("(", ",", ")")
  }
  
  case class ArrExpr(val elements: List[Expr]) extends BaseExpr {
    override def apply() = elements map (_())
    def defaultSQL = elements map { _.sql } mkString ("(", ", ", ")")
    override def toString = elements map { _.toString } mkString ("[", ", ", "]")
  }

  case class SelectExpr(val tables: List[Table], val filter: List[Expr], val cols: List[ColExpr],
    val distinct: Boolean, val group: Expr, val order: List[Expr],
    offset: Expr, limit: Expr, aliases: Map[String, Table]) extends BaseExpr {
    override def apply() = {
      org.tresql.Query.sel(sql, cols, QueryBuilder.this.bindVariables, env,
        QueryBuilder.this.allCols, QueryBuilder.this.identAll, QueryBuilder.this.externalFunction)
    }
    lazy val defaultSQL = "select " + (if (distinct) "distinct " else "") +
      (if (cols == null) "*" else sqlCols) + " from " + tables.head.sqlName + join(tables) +
      //(filter map where).getOrElse("")
      (if (filter == null) "" else " where " + where) +
      (if (group == null) "" else " group by " + group.sql) +
      (if (order == null) "" else " order by " + (order map (_.sql)).mkString(", ")) +
      (if (offset == null) "" else " offset " + offset.sql) +
      (if (limit == null) "" else " limit " + limit.sql)
    def sqlCols = cols.filter(!_.separateQuery).map(_.sql).mkString(", ")
    def join(tables: List[Table]): String = {
      //used to find table if alias join is used
      def find(t: Table) = t match {
        case t @ Table(IdentExpr(n), null, _, _, _) => aliases.getOrElse(n.mkString("."), t)
        case t => t
      }
      (tables: @unchecked) match {
        case t :: Nil => ""
        case t :: l => (Option(" " + l.head.sqlJoin(find(t))) filter (_.size > 1) getOrElse "") + join(l)
      }
    }
    def where = filter match {
      //primary key equals search
      case List(_: ConstExpr | _: VarExpr | _: ResExpr) => tables(tables.size - 1).aliasOrName + "." +
        env.table(tables(tables.size - 1).name).key.cols(0) + " = " + filter(0).sql
      //normal filter expression
      case f :: Nil => (if (f.exprType == classOf[SelectExpr]) "exists " else "") + f.sql
      //primary key in search
      case l => tables(tables.size - 1).aliasOrName + "." +
        env.table(tables(tables.size - 1).name).key.cols(0) +
        " in(" + (l map { _.sql }).mkString(",") + ")"
    }
    val queryDepth = QueryBuilder.this.queryDepth
    override def toString = sql + " (" + QueryBuilder.this + ")\n" +
      (if (cols != null) cols.filter(_.separateQuery).map {
        "    " * (queryDepth + 1) + _.toString
      }.mkString
      else "")
  }
  case class Table(val table: Expr, val alias: String, val join: TableJoin, val outerJoin: String,
    val nullable: Boolean) {
    def name = table.sql
    def sqlName = name + (if (alias != null) " " + alias else "")
    def aliasOrName = if (alias != null) alias else name
    def sqlJoin(joinTable: Table) = {
      def joinPrefix(implicitLeftJoinPossible: Boolean) = (outerJoin match {
        case "l" => "left "
        case "r" => "right "
        case _ if (implicitLeftJoinPossible && nullable) => "left "
        case _ => ""
      }) + "join "
      def fkJoin(i: IdentExpr) = sqlName + " on " + i.sql + " = " +
        aliasOrName + "." + env.table(name).key.cols.mkString
      def defaultJoin(jcols: (List[String], List[String])) = {
        //may be default join columns had been calculated during table build implicit left outer join calculation 
        val j = if (jcols != null) jcols else env.join(joinTable.name, name)
        (j._1 zip j._2 map { t =>
          joinTable.aliasOrName + "." + t._1 + " = " + aliasOrName + "." + t._2
        }) mkString " and "
      }
      join match {
        //no join (used for table alias join)
        case TableJoin(_, _, true, _) => ""
        //product join
        case TableJoin(false, ArrExpr(Nil), _, _) => ", " + sqlName
        //foreign key join shortcut syntax
        case TableJoin(false, e @ IdentExpr(_), _, _) => joinPrefix(true) + fkJoin(e)
        //normal join
        case TableJoin(false, e, _, _) => joinPrefix(false) + sqlName + " on " + e.sql
        //default join
        case TableJoin(true, null, _, dj) => joinPrefix(true) + sqlName + " on " + defaultJoin(dj)
        //default join with additional expression
        case TableJoin(true, j: Expr, _, dj) => joinPrefix(true) + sqlName + " on " +
          defaultJoin(dj) + " and " + (j match {
            //primary key equals search
            case _: ConstExpr | _: VarExpr | _: ResExpr => joinTable.aliasOrName + "." +
              env.table(joinTable.name).key.cols(0) + " = " + j.sql
            //primary key in search
            case ArrExpr(l) => joinTable.aliasOrName + "." +
              env.table(joinTable.name).key.cols(0) + (l map (_.sql) mkString (" in(", ", ", ")"))
            //normal join expression
            case e => (if (e.exprType == classOf[SelectExpr]) "exists " else "") + e.sql
          })
      }
    }
  }
  case class TableJoin(val default: Boolean, val expr: Expr, val noJoin: Boolean,
    defaultJoinCols: (List[String], List[String]))
  case class ColExpr(val col: Expr, val alias: String, val typ: String, sepQuery: Boolean = false, 
      hidden: Boolean = false)
    extends PrimitiveExpr {
    val separateQuery = sepQuery || QueryBuilder.this.separateQueryFlag
    if (!QueryBuilder.this.allCols) QueryBuilder.this.allCols = col.isInstanceOf[AllExpr]
    def defaultSQL = col.sql + (if (alias != null) " " + alias else "")
    def name = if (alias != null) alias.stripPrefix("\"").stripSuffix("\"") else col match {
      case IdentExpr(n) => n(n.length - 1)
      case _ => null
    }
    override def toString = col.toString + (if (alias != null) " " + alias else "")
  }
  case class HiddenColRefExpr(expr: Expr, resType: Class[_]) extends PrimitiveExpr {
    override def apply() = {
      val (res, idx) = (env(0), env(expr))
      if (resType == classOf[Int]) res.int(idx)
      else if (resType == classOf[Long]) res.long(idx)
      else if (resType == classOf[Double]) res.double(idx)
      else if (resType == classOf[Boolean]) res.boolean(idx)
      else if (resType == classOf[BigDecimal]) res.bigdecimal(idx)
      else if (resType == classOf[String]) res.string(idx)
      else if (resType == classOf[java.util.Date]) res.date(idx)
      else if (resType == classOf[java.sql.Date]) res.date(idx)
      else if (resType == classOf[java.sql.Timestamp]) res.timestamp(idx)
      else if (resType == classOf[java.lang.Integer]) res.jInt(idx)
      else if (resType == classOf[java.lang.Long]) res.jLong(idx)
      else if (resType == classOf[java.lang.Double]) res.jDouble(idx)
      else if (resType == classOf[java.lang.Boolean]) res.jBoolean(idx)
      else if (resType == classOf[java.math.BigDecimal]) res.bigdecimal(idx) match {
        case null => null
        case bd => bd.bigDecimal
      }
    }
    override def defaultSQL = error("not implemented")
    override def toString = expr + ":" + resType
  }  
  case class IdentExpr(val name: List[String]) extends PrimitiveExpr {
    def defaultSQL = name.mkString(".")
  }
  case class Order(val ordExprs: (Null, List[Expr], Null), val asc: Boolean) extends PrimitiveExpr {
    def defaultSQL = (ordExprs._2 map (_.sql)).mkString(",") + (if (asc) " asc" else " desc") +
      (if (ordExprs._1 != null) " nulls first" else if (ordExprs._3 != null) " nulls last" else "")
  }
  case class Group(val groupExprs: List[Expr], val having: Expr) extends PrimitiveExpr {
    def defaultSQL = (groupExprs map (_.sql)).mkString(",") +
      (if (having != null) " having " + having.sql else "")
  }

  class InsertExpr(table: IdentExpr, alias: String, val cols: List[Expr], val vals: List[Expr])
    extends DeleteExpr(table, alias, null) {
    override protected def _sql = "insert into " + table.sql + (if (alias == null) "" else " " + alias) +
      " (" + cols.map(_.sql).mkString(", ") + ")" + (vals match {
        case v :: Nil => " values " + v.sql
        case _ => " values (" + vals.map(_.sql).mkString(", ") + ")"
      })
  }
  class UpdateExpr(table: IdentExpr, alias: String, filter: List[Expr], val cols: List[Expr],
    val vals: List[Expr]) extends DeleteExpr(table, alias, filter) {
    if (cols.length != vals.length) error("update statement columns and values count differ: " +
      cols.length + "," + vals.length)
    override protected def _sql = "update " + table.sql + (if (alias == null) "" else " " + alias) +
      " set " + (cols zip vals map { v => v._1.sql + " = " + v._2.sql }).mkString(", ") +
      (if (filter == null) "" else " where " + where)
  }
  class DeleteExpr(val table: IdentExpr, val alias: String, val filter: List[Expr]) extends BaseExpr {
    override def apply() = {
      val r = org.tresql.Query.update(sql, QueryBuilder.this.bindVariables, env)
      executeChildren match {
        case Nil => r
        case x => List(r, x)
      }
    }
    protected def _sql = "delete from " + table.sql + (if (alias == null) "" else " " + alias) +
      (if (filter == null || filter.size == 0) "" else " where " + where)
    lazy val defaultSQL = _sql
    def where = filter match {
      case (c @ ConstExpr(x)) :: Nil => Option(alias).getOrElse(table.sql) + "." +
        env.table(table.sql).key.cols(0) + " = " + c.sql
      case (v @ VarExpr(x, _)) :: Nil => Option(alias).getOrElse(table.sql) + "." +
        env.table(table.sql).key.cols(0) + " = " + v.sql
      case f :: Nil => (if (f.exprType == classOf[SelectExpr]) "exists " else "") + f.sql
      case l => Option(alias).getOrElse(table.sql) + "." + env.table(table.sql).key.cols(0) + " in(" +
        (l map { _.sql }).mkString(",") + ")"
    }
  }

  case class BracesExpr(val expr: Expr) extends BaseExpr {
    override def apply() = expr()
    def defaultSQL = "(" + expr.sql + ")"
    override def exprType = expr.exprType
  }

  abstract class BaseExpr extends PrimitiveExpr {
    override def apply(params: Map[String, Any]): Any = {
      env update params
      apply()
    }
    override def close = {
      env.closeStatement
      childUpdates foreach { t => t._1.close }
    }
  }

  abstract class PrimitiveExpr extends Expr {
    def builder = QueryBuilder.this
  }

  private def executeChildren = {
    childUpdates.map {
      case (ex, null) => ex()
      case (ex, n) if (!env.contains(n)) => ex()
      case (ex, n) => env(n) match {
        case m: Map[String, _] => ex(m)
        case t: scala.collection.Traversable[Map[String, _]] => t map { ex(_) }
        case a: Array[Map[String, _]] => (a map { ex(_) }).toList
        case x => ex()
      }
    }.toList
  }

  //DML statements are defined outsided buildInternal method since they are called from other QueryBuilder
  private def buildInsert(table: Ident, alias: String, cols: List[Col], vals: List[Arr]) = {
    new InsertExpr(IdentExpr(table.ident), alias, cols map (buildInternal(_, COL_CTX)) filter {
      case x @ ColExpr(IdentExpr(_), _, _, _, _) => true
      case e: ColExpr => this.childUpdates += { (e.col, e.name) }; false
    }, vals map { buildInternal(_, VALUES_CTX) })
  }
  private def buildUpdate(table: Ident, alias: String, filter: Arr, cols: List[Col], vals: Arr) = {
    new UpdateExpr(IdentExpr(table.ident), alias, if (filter != null)
      filter.elements map { buildInternal(_, WHERE_CTX) } else null,
      cols map (buildInternal(_, COL_CTX)) filter {
        case ColExpr(IdentExpr(_), _, _, _, _) => true
        case e: ColExpr => this.childUpdates += { (e.col, e.name) }; false
      },
      vals.elements map { buildInternal(_, VALUES_CTX) })
  }
  private def buildDelete(table: Ident, alias: String, filter: Arr) = {
    new DeleteExpr(IdentExpr(table.ident), alias,
      if (filter != null) filter.elements map { buildInternal(_, WHERE_CTX) } else null)
  }

  private def buildInternal(parsedExpr: Any, parseCtx: String = ROOT_CTX): Expr = {
    def buildSelect(q: Query) = {
      val tablesAndAliases = buildTables(q.tables)
      val sel = SelectExpr(tablesAndAliases._1, //tables
        if (q.filter != null) { //filter
          val f = (q.filter.elements map { buildInternal(_, WHERE_CTX) }).filter(_ != null)
          if (f.length > 0) f else null
        } else null,
        buildCols(q.cols), q.distinct, buildInternal(q.group), //cols, distinct, group
        if (q.order != null) q.order map { buildInternal(_, ORD_CTX) } else null, //order
        buildInternal(q.offset, LIMIT_CTX), buildInternal(q.limit, LIMIT_CTX), //offset, limit
        tablesAndAliases._2)
      //if select expression is subquery in others expression where clause, had where clause itself
      //and where clause was removed due to unbound optional variables remove subquery itself
      if (ctxStack.head == WHERE_CTX && q.filter != null && q.filter.elements != Nil &&
        sel.filter == null) null else sel
    }
    //build tables, set nullable flag for tables right to default join or foreign key shortcut join,
    //which is used for implicit left outer join, create aliases map
    def buildTables(tables: List[Obj]) = {
      ((tables map buildTable).foldLeft((List[Table]() -> Map[String, Table]())) { (ts, t) =>
        val nt = ts._1.headOption.map(pt => (pt, t) match {
          case (Table(IdentExpr(ptn), ptna, _, _, _), Table(IdentExpr(n), _, j, null, false)) =>
            //get previous table from aliases map if exists
            val prevTable = ts._2.get(ptn.mkString(".")).getOrElse(pt)
            j match {
              //foreign key of shortcut join must come from previous table i.e. ptn 
              case TableJoin(false, IdentExpr(fk), _, _) if (fk.size == 1 ||
                fk.dropRight(1) == (if (ptna == null) ptn else List(ptna))) =>
                env.colOption(prevTable.name, fk.last).map(col =>
                  //set current table to nullable if foreign key column or previous table is nullable
                  if (prevTable.nullable || col.nullable) t.copy(nullable = true) else t).getOrElse(t)
              //default join
              case TableJoin(true, _, _, _) =>
                val dj = env.join(prevTable.name, t.name)
                if (prevTable.nullable || dj._1.exists(env.col(prevTable.name, _).nullable))
                  t.copy(join = t.join.copy(defaultJoinCols = dj), nullable = true)
                else t.copy(join = t.join.copy(defaultJoinCols = dj))
              case _ => t
            }
          case _ => t
        }).getOrElse(t)
        (nt :: ts._1) -> (nt match {
          case Table(IdentExpr(n), a, _, _, _) if (a != null && !ts._2.contains(a)) =>
            ts._2 + (a -> nt)
          case _ => ts._2
        })
      }) match {
        case (tables: List[Table], aliases: Map[String, Table]) => (tables.reverse, aliases)
      }
    }
    def buildTable(t: Obj) = Table(buildInternal(t.obj, TABLE_CTX), t.alias,
      if (t.join != null) TableJoin(t.join.default, buildInternal(t.join.expr, JOIN_CTX),
        t.join.noJoin, null)
      else null, t.outerJoin, t.nullable)
    def buildColumnIdentOrBracesExpr(c: Obj) = c match {
      case Obj(Ident(i), _, _, _, _) => IdentExpr(i)
      case Obj(b @ Braces(_), _, _, _, _) => buildInternal(b, parseCtx)
      case o => error("unsupported column definition at this place: " + o)
    }
    def buildIdentOrBracesExpr(i: Obj) = i match {
      case Obj(Ident(i), null, _, _, _) => IdentExpr(i)
      case Obj(b @ Braces(_), _, _, _, _) => buildInternal(b, parseCtx)
      case o => error("unsupported column definition at this place: " + o)
    }
    def buildCols(cols: List[Col]) = if (cols == null) List(ColExpr(AllExpr(), null, null)) else {
      val colExprs = cols.map(buildInternal(_, COL_CTX).asInstanceOf[ColExpr])
      if (colExprs.exists {
        case ColExpr(FunExpr(n, _), _, _, _, _) => Env.isDefined(n)
        case _ => false
      }) (colExprs flatMap { //external function found
        case ColExpr(FunExpr(n, pars), a, t, _, _) if Env.isDefined(n) =>
          val m = Env.functions.flatMap(_.getClass.getMethods.filter(m=> m.getName == n &&
              m.getParameterTypes.size == pars.size).headOption).getOrElse(
                  error("External function " + n + " with " + pars.size + " parameters not found"))
          QueryBuilder.this.externalFunction = true
          ColExpr(ExternalFunExpr(n, m.getParameterTypes.toList.zip(pars).map(tp=>
            HiddenColRefExpr(tp._2, tp._1)), m), a, t, true) :: pars.map(
                ColExpr(_, null, null, hidden = true))
        case e => List(e)
      }).groupBy (_.hidden) match { //put hidden columns at the end
        case m: Map[Boolean, ColExpr] => m(false) ++ m.getOrElse(true, Nil)
      } else colExprs
    }
    
    ctxStack ::= parseCtx
    try {
      parsedExpr match {
        case x: Number => ConstExpr(x)
        case x: String => ConstExpr(x)
        case x: Boolean => ConstExpr(x)
        case Null() => ConstExpr(null)
        //variable assignment
        case BinOp("=", Variable(n, o), v) if (parseCtx == ROOT_CTX) =>
          AssignExpr(n, buildInternal(v, parseCtx))
        //insert
        case Insert(t, a, c, v) =>
          val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
          val ex = b.buildInsert(t, a, c, v)
          this.separateQueryFlag = true; this.bindIdx = b.bindIdx; ex
        //update
        case Update(t, a, f, c, v) =>
          val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
          val ex = b.buildUpdate(t, a, f, c, v)
          this.separateQueryFlag = true; this.bindIdx = b.bindIdx; ex
        //delete
        case Delete(t, a, f) =>
          val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
          val ex = b.buildDelete(t, a, f)
          this.separateQueryFlag = true; this.bindIdx = b.bindIdx; ex
        //child query
        case UnOp("|", oper) =>
          val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth + 1, bindIdx)
          val ex = b.buildInternal(oper, QUERY_CTX)
          this.separateQueryFlag = true; this.bindIdx = b.bindIdx; ex
        case t: Obj => parseCtx match {
          case ROOT_CTX => t match {
            case Obj(b @ Braces(_), _, _, _, _) => buildInternal(b, parseCtx)
            case _ =>
              val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
              val ex = b.buildInternal(t, QUERY_CTX); this.bindIdx = b.bindIdx; ex
          }
          case QUERY_CTX => t match {
            case Obj(b @ Braces(_), _, _, _, _) => buildInternal(b, parseCtx)
            case _ =>
              val tablesAndAliases = buildTables(List(t))
              SelectExpr(tablesAndAliases._1, null,
                List(ColExpr(AllExpr(), null, null)), false, null, null, null, null, tablesAndAliases._2)
          }
          case TABLE_CTX =>
            val tablesAndAliases = buildTables(List(t))
            SelectExpr(tablesAndAliases._1, null,
              List(ColExpr(AllExpr(), null, null)), false, null, null, null, null, tablesAndAliases._2)
          case COL_CTX => buildColumnIdentOrBracesExpr(t)
          case _ => buildIdentOrBracesExpr(t)
        }
        case q: Query => parseCtx match {
          case ROOT_CTX =>
            val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
            val ex = b.buildInternal(q, QUERY_CTX); this.bindIdx = b.bindIdx; ex
          case _ => buildSelect(q)
        }
        case UnOp(op, oper) =>
          val o = buildInternal(oper, parseCtx)
          if (o == null) null else UnExpr(op, o)
        case e @ BinOp(op, lop, rop) => parseCtx match {
          case ROOT_CTX =>
            val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
            val ex = b.buildInternal(e, QUERY_CTX); this.bindIdx = b.bindIdx; ex
          case ctx =>
            val l = buildInternal(lop, ctx)
            val r = buildInternal(rop, ctx)
            if (l != null && r != null) BinExpr(op, l, r) else if (op == "&" || op == "|")
              if (l != null) l else if (r != null) r else null
            else null
        }
        case In(lop, rop, not) =>
          val l = buildInternal(lop, parseCtx)
          if (l == null) null else {
            val r = rop.map(buildInternal(_, parseCtx)).filter(_ != null)
            if (r.size == 0) null else InExpr(l, r, not)
          }
        case Fun(n, pl: List[_]) =>
          val pars = pl map { buildInternal(_, FUN_CTX) }
          if (pars.exists(_ == null)) null else FunExpr(n, pars)
        case Ident(i) => IdentExpr(i)
        case IdentAll(i) => IdentAllExpr(i.ident)
        case Arr(l: List[_]) => ArrExpr(l map { buildInternal(_, parseCtx) })
        case Variable("?", o) =>
          this.bindIdx += 1; VarExpr(this.bindIdx.toString, o)
        case Variable(n, o) => if (!env.reusableExpr && o && !(env contains n)) null else VarExpr(n, o)
        case Id(seq) => IdExpr(seq)
        case IdRef(seq) => IdRefExpr(seq)
        case Res(r, c) => ResExpr(r, c)
        case Col(c, a, t) => 
          separateQueryFlag = false
          ColExpr(buildInternal(c, parseCtx), a, t)
        case Grp(cols, having) => Group(cols map { buildInternal(_, GROUP_CTX) },
          buildInternal(having, HAVING_CTX))
        case Ord(cols, asc) => Order((cols._1, cols._2 map { buildInternal(_, parseCtx) },
          cols._3), asc)
        case All() => AllExpr()
        case null => null
        case Braces(expr) =>
          val e = buildInternal(expr, parseCtx)
          if (e == null) null else BracesExpr(e)
        case x => ConstExpr(x)
      }
    } finally ctxStack = ctxStack.tail
  }

  private def build(ex: String): Expr = {
    buildInternal(parseExp(ex))
  }

  private def build(ex: Exp): Expr = {
    buildInternal(ex)
  }

  override def toString = "QueryBuilder: " + queryDepth

}

object QueryBuilder {
  def apply(ex: String, env: Env = Env(Map(), true)): Expr = {
    new QueryBuilder(env).build(ex)
  }
  def apply(ex: Exp, env: Env): Expr = {
    new QueryBuilder(env).build(ex)
  }
}

abstract class Expr extends (() => Any) with Ordered[Expr] {

  type Number = BigDecimal

  def *(e: Expr) = this() match {
    case x: Number => x * e().asInstanceOf[Number]
  }
  def /(e: Expr) = this() match {
    case x: Number => x / e().asInstanceOf[Number]
  }
  def +(e: Expr) = this() match {
    case x: Number => x + e().asInstanceOf[Number]
    case x: String => x + e().asInstanceOf[String]
  }
  def -(e: Expr) = this() match {
    case x: Number => x - e().asInstanceOf[Number]
  }
  def &(e: Expr) = this() match {
    case x: Boolean => x & e().asInstanceOf[Boolean]
  }
  def |(e: Expr) = this() match {
    case x: Boolean => x | e().asInstanceOf[Boolean]
    case r1: Result => {
      val r2 = e().asInstanceOf[Result]
      val b = new scala.collection.mutable.ListBuffer[Any]
      r1 foreach { r =>
        b += r1.content ++ (if (r2.hasNext) { r2.next; r2.content } else Nil)
      }
      b.toList
    }
  }
  def compare(that: Expr) = {
    this() match {
      case x: Number => x.compare(that().asInstanceOf[Number])
      case x: String => x.compare(that().asInstanceOf[String])
      case null => if (that() == null) 0 else -1
      case x: Boolean => if (x == that()) 0 else -1
      case x: Expr => if (super.equals(that)) 0 else -1
    }
  }
  //use object equals method for hash map
  override def equals(that: Any) = super.equals(that)
  
  def in(inList: List[Expr]) = inList.contains(this)
  def notIn(inList: List[Expr]) = !inList.contains(this)

  def defaultSQL: String
  def builder: QueryBuilder
  def sql = if (builder.env.dialect != null) builder.env.dialect(this) else defaultSQL

  def apply(): Any = this
  def apply(params: Seq[Any]): Any = apply(org.tresql.Query.normalizePars(params))
  def apply(params: Map[String, Any]): Any = this
  def select(params: Any*): Result = select(org.tresql.Query.normalizePars(params))
  def select(params: Map[String, Any]): Result = apply(params).asInstanceOf[Result]
  def foreach(params: Any*)(f: (RowLike) => Unit = (row) => ()) {
    (if (params.size == 1) params(0) match {
      case l: List[_] => select(l)
      case m: Map[String, _] => select(m)
      case x => select(x)
    }
    else select(params)) foreach f
  }
  def close: Unit = error("Close method not implemented in subclass of Expr: " + getClass)
  def exprType: Class[_] = this.getClass
  override def toString = defaultSQL
}