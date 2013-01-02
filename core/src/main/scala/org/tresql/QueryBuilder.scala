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
    def defaultSQL = name.mkString(".") + ".*"
  }

  case class VarExpr(val name: String, val opt: Boolean) extends BaseExpr {
    override def apply() = env(name)
    var binded = false
    def defaultSQL = {
      if (!binded) { QueryBuilder.this._bindVariables += this; binded = true }
      if (!env.reusableExpr && (env contains name)) {
        env(name) match {
          case l: scala.collection.Traversable[_] => "?," * (l size) dropRight 1
          case a: Array[_] => "?," * (a size) dropRight 1
          case _ => "?"
        }
      } else "?"
    }
    override def toString = if (env contains name) name + " = " + env(name) else name
  }
  
  case class IdExpr(val seqName: String) extends BaseExpr {
    override def apply() = env.nextId(seqName)
    var binded = false
    def defaultSQL = {
      if (!binded) { QueryBuilder.this._bindVariables += this; binded = true }
      "?"
    }
  }

  case class IdRefExpr(val seqName: String) extends BaseExpr {
    override def apply() = env.currId(seqName)
    var binded = false
    def defaultSQL = {
      if (!binded) { QueryBuilder.this._bindVariables += this; binded = true }
      "?"
    }
  }
  
  class ResExpr(val nr: Int, val col: Any) extends PrimitiveExpr {
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

  class AssignExpr(val variable: String, val value: Expr) extends BaseExpr {
    //add variable to environment so that variable is found when referenced in further expressions
    env(variable) = null
    override def apply() = {
      env(variable) = value()
      env(variable)
    }
    def defaultSQL = error("Cannot construct sql statement from assignment expression")
    override def toString = variable + " = " + value
  }

  class UnExpr(val op: String, val operand: Expr) extends BaseExpr {
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
        case "&&" => org.tresql.Query.select(sql, selCols(lop),
          QueryBuilder.this.bindVariables, env, QueryBuilder.this.allCols, QueryBuilder.this.identAll)
        case "++" => org.tresql.Query.select(sql, selCols(lop),
          QueryBuilder.this.bindVariables, env, QueryBuilder.this.allCols, QueryBuilder.this.identAll)
        case "+" => if (exprType == classOf[SelectExpr])
          org.tresql.Query.select(sql, selCols(lop), QueryBuilder.this.bindVariables, env,
            QueryBuilder.this.allCols, QueryBuilder.this.identAll)
        else lop + rop
        case "-" => if (exprType == classOf[SelectExpr])
          org.tresql.Query.select(sql, selCols(lop), QueryBuilder.this.bindVariables, env,
            QueryBuilder.this.allCols, QueryBuilder.this.identAll)
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
      case "-" => lop.sql + (if (exprType == classOf[SelectExpr]) " minus " else " - ") + rop.sql
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
      case "in" => lop.sql + " in " + rop.sql
      case "!in" => lop.sql + " not in " + rop.sql
      case _ => error("unknown operation " + op)
    }
    override def exprType: Class[_] = if (List("&&", "++", "+", "-", "*", "/") exists (_ == op)) {
      if (lop.exprType == rop.exprType) lop.exprType else super.exprType
    } else classOf[ConstExpr]
  }

  case class FunExpr(val name: String, val params: List[Expr]) extends BaseExpr {
    override def apply() = {
      val p = params map (_())
      val ts = p map (_.asInstanceOf[AnyRef].getClass)
      if (Env.isDefined(name)) {
        try {
          val m = Functions.getClass.getMethod(name, ts: _*)
          m.invoke(Functions, p.asInstanceOf[List[Object]]: _*)
        } catch {
          case ex: NoSuchMethodException => {
            val ms = Functions.getClass.getMethods filter { m =>
              val par = m.getParameterTypes
              m.getName == name && par.length == 1 && par(0).isInstance(p)
            }
            if (ms.length > 0) ms(0).invoke(Functions, List(p).asInstanceOf[List[Object]]: _*)
            else org.tresql.Query.call("{call " + sql + "}", QueryBuilder.this.bindVariables, env)
          }
        }
      } else org.tresql.Query.call("{call " + sql + "}", QueryBuilder.this.bindVariables, env)
    }
    def defaultSQL = name + (params map (_.sql)).mkString("(", ",", ")")
    override def toString = name + (params map (_.toString)).mkString("(", ",", ")")
  }

  case class ArrExpr(val elements: List[Expr]) extends BaseExpr {
    override def apply() = elements map (_())
    def defaultSQL = elements map { _.sql } mkString ("(", ", ", ")")
    override def toString = elements map { _.toString } mkString ("[", ", ", "]")
  }

  class SelectExpr(val tables: List[Table], val filter: List[Expr], val cols: List[ColExpr],
    val distinct: Boolean, val group: Expr, val order: List[Expr],
    offset: Expr, limit: Expr) extends BaseExpr {
    val aliases: Map[Any, Table] = {
      var al = Set[String]()
      tables.flatMap {
        //primary key shortcut join aliases checked
        case tb@Table(IdentExpr(t), null, TableJoin(_, ArrExpr(l), _), _) => {
          l map { 
            case AliasIdentExpr(_, a) => al += a; a -> tb.copy(alias = a)
            case _ => val s = t.mkString("."); (if (al(s)) null else s) -> tb
          }
        }
        case tb@Table(IdentExpr(t), null, _, _) => val s = t.mkString("."); List((if (al(s)) null else s) -> tb)
        case tb@Table(t, null, _, _) => List((null, tb))
        case tb@Table(t, a, _, _) => al += a; List(a -> tb)
      }.filter(_._1 != null).toMap
    }
    override def apply() = {
      org.tresql.Query.select(sql, cols, QueryBuilder.this.bindVariables, env,
        QueryBuilder.this.allCols, QueryBuilder.this.identAll)
    }
    lazy val defaultSQL = "select " + (if (distinct) "distinct " else "") +
      (if (cols == null) "*" else sqlCols) + " from " + tables.head.sqlName + join(tables) +
      //(filter map where).getOrElse("")
      (if (filter == null) "" else " where " + where) +
      (if (group == null) "" else " group by " + group.sql) +
      (if (order == null) "" else " order by " + (order map (_.sql)).mkString(", ")) +
      (if (offset == null) "" else " offset " + offset.sql) +
      (if (limit == null) "" else " limit " + limit.sql)
    def sqlCols = cols.filter(!_.separateQuery).map(_.sql).mkString(",")
    def join(tables: List[Table]): String = {
      //used to find table if alias join is used
      def find(t: Table) = t match {
        case t@Table(IdentExpr(n), null, _, _) => aliases.getOrElse(n.mkString("."), t)
        case t => t
      }
      (tables: @unchecked) match {
        case t :: Nil => ""
        case t :: l => " " + l.head.sqlJoin(find(t)) + join(l)
      }
    }
    def where = filter match {
      case List(_:ConstExpr | _:VarExpr | _:ResExpr) => tables(0).aliasOrName + "." +
        env.table(tables(0).name).key.cols(0) + " = " + filter(0).sql
      case f :: Nil => (if (f.exprType == classOf[SelectExpr]) "exists " else "") + f.sql
      case l => tables(0).aliasOrName + "." + env.table(tables(0).name).key.cols(0) + " in(" +
        (l map { _.sql }).mkString(",") + ")"
    }
    val queryDepth = QueryBuilder.this.queryDepth
    override def toString = sql + " (" + QueryBuilder.this + ")\n" +
      (if (cols != null) cols.filter(_.separateQuery).map {
        "    " * (queryDepth + 1) + _.toString
      }.mkString
      else "")
  }
  case class Table(val table: Expr, val alias: String, val join: TableJoin, val outerJoin: String) {
    def name = table.sql
    def sqlName = name + (if (alias != null) " " + alias else "")
    def aliasOrName = if (alias != null) alias else name
    def sqlJoin(joinTable: Table) = {
      def sqlJoin(jl: List[Expr]) = (jl map { j =>
        joinPrefix + (j match {
          //primary key join shortcut syntax
          case i@IdentExpr(_) => sqlName + " on " + i.sql + " = " +
            aliasOrName + "." + env.table(name).key.cols.mkString
          //primary key join shortcut syntax
          case AliasIdentExpr(i, a) => name + " " + a + " on " +
            i.mkString(".") + " = " + a + "." + env.table(name).key.cols.mkString
          case e => sqlName + " on " + e.sql
        })
      }) mkString " "
      def joinPrefix = (outerJoin match {
        case "l" => "left "
        case "r" => "right "
        case null => ""
      }) + "join "
      def defaultJoin = {
        val j = env.join(name, joinTable.name)
        (j._1 zip j._2 map { t =>
          aliasOrName + "." + t._1 + " = " +
            joinTable.aliasOrName + "." + t._2
        }) mkString " and "
      }
      join match {
        //no join (used for table alias join)
        case TableJoin(_, _, true) => ""
        //product join
        case TableJoin(false, ArrExpr(Nil), _) => ", " + sqlName
        //normal join, primary key join shortcut syntax
        case TableJoin(false, ArrExpr(l), _) => sqlJoin(l)
        //default join
        case TableJoin(true, null, _) => joinPrefix + sqlName + " on " + defaultJoin
        //default join with additional expression
        case TableJoin(true, j: Expr, _) => joinPrefix + sqlName + " on " + defaultJoin +
          " and " + j.sql
      }
    }
  }
  case class TableJoin(val default: Boolean, val expr: Expr, val noJoin: Boolean)
  case class ColExpr(val col: Expr, val alias: String) extends PrimitiveExpr {
    val separateQuery = QueryBuilder.this.separateQueryFlag
    if (!QueryBuilder.this.allCols) QueryBuilder.this.allCols = col.isInstanceOf[AllExpr]    
    def defaultSQL = col.sql + (if (alias != null) " " + alias else "")
    def name = if (alias != null) alias.stripPrefix("\"").stripSuffix("\"") else col match {
      case IdentExpr(n) => n(n.length - 1)
      case _ => null
    }    
    override def toString = col.toString + (if (alias != null) " " + alias else "")
  }
  case class IdentExpr(val name: List[String]) extends PrimitiveExpr {
    def defaultSQL = name.mkString(".")
  }
  //this is used for alias join shortcut syntax
  case class AliasIdentExpr(val name: List[String], val alias: String) extends PrimitiveExpr {
    def defaultSQL = "AliasIdentExpr(" + name + ", " + alias + ")"
  }
  class Order(val ordExprs: (Null, List[Expr], Null), val asc: Boolean) extends PrimitiveExpr {
    def defaultSQL = (ordExprs._2 map (_.sql)).mkString(",") + (if (asc) " asc" else " desc") +
      (if (ordExprs._1 != null) " nulls first" else if (ordExprs._3 != null) " nulls last" else "")
  }
  class Group(val groupExprs: List[Expr], val having: Expr) extends PrimitiveExpr {
    def defaultSQL = (groupExprs map (_.sql)).mkString(",") +
      (if (having != null) " having " + having.sql else "")
  }

  class InsertExpr(table: IdentExpr, val cols: List[Expr], val vals: List[Expr])
    extends DeleteExpr(table, null) {
    override protected def _sql = "insert into " + table.sql +
      "(" + cols.map(_.sql).mkString(",") + ")" + (vals match {
      case v :: Nil => " values " + v.sql
      case _ => " values (" + vals.map(_.sql).mkString(",") + ")" 
    })
  }
  class UpdateExpr(table: IdentExpr, filter: List[Expr], val cols: List[Expr],
    val vals: List[Expr]) extends DeleteExpr(table, filter) {
    if (cols.length != vals.length) error("update statement columns and values count differ: " +
      cols.length + "," + vals.length)
    override protected def _sql = "update " + table.sql + " set " +
      (cols zip vals map { v => v._1.sql + " = " + v._2.sql }).mkString(",") +
      (if (filter == null) "" else " where " + where)
  }
  class DeleteExpr(val table: IdentExpr, val filter: List[Expr]) extends BaseExpr {
    override def apply() = {
      val r = org.tresql.Query.update(sql, QueryBuilder.this.bindVariables, env)
      executeChildren match {
        case Nil => r
        case x => List(r, x)
      }
    }
    protected def _sql = "delete from " + table.sql +
      (if (filter == null || filter.size == 0) "" else " where " + where)
    lazy val defaultSQL = _sql
    def where = filter match {
      case (c @ ConstExpr(x)) :: Nil => table.sql + "." +
        env.table(table.sql).key.cols(0) + " = " + c.sql
      case (v @ VarExpr(x, _)) :: Nil => table.sql + "." +
        env.table(table.sql).key.cols(0) + " = " + v.sql
      case f :: Nil => (if (f.exprType == classOf[SelectExpr]) "exists " else "") + f.sql
      case l => table.sql + "." + env.table(table.sql).key.cols(0) + " in(" +
        (l map { _.sql }).mkString(",") + ")"
    }
  }

  class BracesExpr(val expr: Expr) extends BaseExpr {
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
      childUpdates foreach {t => t._1.close}
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
        case t: scala.collection.Traversable[Map[String, _]] => t map {ex(_)}
        case a: Array[Map[String, _]] => (a map {ex(_)}).toList
        case x => ex()
      }
    }.toList
  }

  //DML statements are defined outsided buildInternal method since they are called from other QueryBuilder
  private def buildInsert(table: Ident, cols: List[Col], vals: List[Arr]) = {
    new InsertExpr(IdentExpr(table.ident), cols map (buildInternal(_, COL_CTX)) filter {
      case x@ColExpr(IdentExpr(_), _) => true
      case e: ColExpr => this.childUpdates += { (e.col, e.name) }; false
    }, vals map { buildInternal(_, VALUES_CTX) })
  }
  private def buildUpdate(table: Ident, filter: Arr, cols: List[Col], vals: Arr) = {
    new UpdateExpr(new IdentExpr(table.ident), if (filter != null)
      filter.elements map { buildInternal(_, WHERE_CTX) } else null,
      cols map (buildInternal(_, COL_CTX)) filter {
        case ColExpr(IdentExpr(_), _) => true
        case e: ColExpr => this.childUpdates += { (e.col, e.name) }; false
      },
      vals.elements map { buildInternal(_, VALUES_CTX) })
  }
  private def buildDelete(table: Ident, filter: Arr) = {
    new DeleteExpr(new IdentExpr(table.ident),
      if (filter != null) filter.elements map { buildInternal(_, WHERE_CTX) } else null)
  }

  private def buildInternal(parsedExpr: Any, parseCtx: String = ROOT_CTX): Expr = {
    def buildSelect(q: Query) = {
      val sel = new SelectExpr(q.tables map buildTable, //tables
        if (q.filter != null) { //filter
          val f = (q.filter.elements map { buildInternal(_, WHERE_CTX) }).filter(_ != null)
          if (f.length > 0) f else null
        } else null,
        if (q.cols != null) q.cols map { buildInternal(_, COL_CTX).asInstanceOf[ColExpr] } //cols 
        else List(new ColExpr(AllExpr(), null)),
        q.distinct, buildInternal(q.group), //distinct, group
        if (q.order != null) q.order map { buildInternal(_, ORD_CTX) } else null, //order
        buildInternal(q.offset, LIMIT_CTX), buildInternal(q.limit, LIMIT_CTX)) //offset, limit
      //if select expression is subquery in others expression where clause, had where clause itself
      //and where clause was removed due to unbound optional variables remove subquery itself
      if (ctxStack.head == WHERE_CTX && q.filter != null && q.filter.elements != Nil &&
        sel.filter == null) null else sel
    }
    def buildTable(t: Obj) = new Table(buildInternal(t.obj, TABLE_CTX), t.alias,
      if (t.join != null) TableJoin(t.join.default, buildInternal(t.join.expr, JOIN_CTX),
        t.join.noJoin)
      else null, t.outerJoin)
    def buildColumnIdent(c: Obj) = c match {
      case Obj(Ident(i), _, _, _) => IdentExpr(i)
      case o => error("unsupported column definition at this place: " + o)
    }
    def buildIdent(i: Obj) = i match {
      case Obj(Ident(i), null, _, _) => IdentExpr(i)
      case Obj(Ident(i), a, _, _) => AliasIdentExpr(i, a)
      case o => error("unsupported column definition at this place: " + o)
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
          new AssignExpr(n, buildInternal(v, parseCtx))
        //insert
        case Insert(t, c, v) => {
          val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
          val ex = b.buildInsert(t, c, v)
          this.bindIdx = b.bindIdx; ex
        }
        //update
        case Update(t, f, c, v) => {
          val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
          val ex = b.buildUpdate(t, f, c, v)
          this.bindIdx = b.bindIdx; ex
        }
        //delete
        case Delete(t, f) => {
          val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
          val ex = b.buildDelete(t, f)
          this.bindIdx = b.bindIdx; ex
        }
        //child query
        case UnOp("|", oper) => {
          val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth + 1, bindIdx)
          val ex = b.buildInternal(oper, QUERY_CTX)
          this.separateQueryFlag = true; this.bindIdx = b.bindIdx; ex
        }
        case t: Obj => parseCtx match {
          case ROOT_CTX => {
            val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
            val ex = b.buildInternal(t, QUERY_CTX); this.bindIdx = b.bindIdx; ex
          }
          case QUERY_CTX | TABLE_CTX => new SelectExpr(List(buildTable(t)), null,
            List(new ColExpr(AllExpr(), null)), false, null, null, null, null)
          case COL_CTX => buildColumnIdent(t)
          case _ => buildIdent(t)
        }
        case q: Query => parseCtx match {
          case ROOT_CTX => {
            val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
            val ex = b.buildInternal(q, QUERY_CTX); this.bindIdx = b.bindIdx; ex
          }
          case _ => buildSelect(q)
        }
        case UnOp(op, oper) => {
          val o = buildInternal(oper, parseCtx)
          if (o == null) null else new UnExpr(op, o)
        }
        case e @ BinOp(op, lop, rop) => parseCtx match {
          case ROOT_CTX => {
            val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
            val ex = b.buildInternal(e, QUERY_CTX); this.bindIdx = b.bindIdx; ex
          }
          case ctx => {
            val l = buildInternal(lop, ctx)
            val r = buildInternal(rop, ctx)
            if (l != null && r != null) new BinExpr(op, l, r) else if (op == "&" || op == "|")
              if (l != null) l else if (r != null) r else null
            else null
          }
        }
        case Fun(n, pl: List[_]) => {
          val pars = pl map { buildInternal(_, parseCtx) }
          if (pars.exists(_ == null)) null else new FunExpr(n, pars)  
        }
        case Ident(i) => IdentExpr(i)
        case IdentAll(i) => {identAll = true; IdentAllExpr(i.ident)}
        case Arr(l: List[_]) => new ArrExpr(l map { buildInternal(_, parseCtx) })
        case Variable("?", o) => this.bindIdx += 1; new VarExpr(this.bindIdx.toString, o)
        case Variable(n, o) => if (!env.reusableExpr && o && !(env contains n)) null else new VarExpr(n, o)
        case Id(seq) => IdExpr(seq)
        case IdRef(seq) => IdRefExpr(seq)
        case Result(r, c) => new ResExpr(r, c)
        case Col(c, a) => { separateQueryFlag = false; new ColExpr(buildInternal(c, parseCtx), a) }
        case Grp(cols, having) => new Group(cols map { buildInternal(_, GROUP_CTX) },
          buildInternal(having, HAVING_CTX))
        case Ord(cols, asc) => new Order((cols._1, cols._2 map { buildInternal(_, parseCtx) },
          cols._3), asc)
        case All() => AllExpr()
        case null => null
        case Braces(expr) => {
          val e = buildInternal(expr, parseCtx)
          if (e == null) null else new BracesExpr(e)
        }
        case x => ConstExpr(x)
      }
    } finally {
      ctxStack = ctxStack.tail
    }
  }

  private def build(ex: String): Expr = {
    buildInternal(parseExp(ex))
  }
  
  private def build(ex:Exp):Expr = {
    buildInternal(ex)
  }
  
  override def toString = "QueryBuilder: " + queryDepth

}

object QueryBuilder {
  def apply(ex: String, env: Env = Env(Map(), true)): Expr = {
    new QueryBuilder(env).build(ex)
  }
  def apply(ex:Exp, env:Env):Expr = {
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
    }
  }
  override def equals(that: Any) = {
    that match {
      case e: Expr => compare(e) == 0
      case _ => false
    }
  }

  def defaultSQL: String
  def builder: QueryBuilder
  def sql = if(builder.env.dialect != null) builder.env.dialect(this) else defaultSQL

  def apply(): Any = error("Must be implemented in subclass")
  def apply(params: Seq[Any]): Any = apply(org.tresql.Query.normalizePars(params))
  def apply(params: Map[String, Any]): Any = error("Must be implemented in subclass")
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
  def close: Unit = error("Must be implemented in subclass")
  def exprType: Class[_] = this.getClass
  override def toString = defaultSQL
}