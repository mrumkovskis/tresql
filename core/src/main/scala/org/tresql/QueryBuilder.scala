package org.tresql

import sys._

class QueryBuilder private (val env: Env, private val queryDepth: Int,
  private var bindIdx: Int) extends EnvProvider {

  import QueryParser._

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

  //context stack as buildInternal method is called
  private var ctxStack = List[String]()
  //bind variables for jdbc prepared statement 
  private val _bindVariables = scala.collection.mutable.ListBuffer[Expr]()
  private lazy val bindVariables = _bindVariables.toList

  //used internally while building expression
  private var separateQueryFlag = false
  //indicate * in column
  private var allCols = false

  case class ConstExpr(val value: Any) extends BaseExpr {
    override def apply() = value
    def sql = value match {
      case v: Number => v.toString
      case v: String => "'" + v + "'"
      case v: Boolean => v.toString
      case null => "null"
    }
  }

  case class AllExpr() extends Expr {
    def sql = "*"
  }

  case class VarExpr(val name: String, val opt: Boolean) extends BaseExpr {
    override def apply() = env(name)
    var binded = false
    def sql = {
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

  class ResExpr(val nr: Int, val col: Any) extends Expr {
    override def apply() = col match {
      case c: List[_] => env(nr)(c.mkString("."))
      case c: String => env(nr)(c)
      case c: Int if (c > 0) => env(nr)(c - 1)
      case c: Int => error("column index in result expression must be greater than 0. Is: " + c)
    }
    var binded = false
    def sql = {
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
    def sql = error("Cannot construct sql statement from assignment expression")
    override def toString = variable + " = " + value
  }

  class UnExpr(val op: String, val operand: Expr) extends BaseExpr {
    override def apply() = op match {
      case "-" => -operand().asInstanceOf[Number]
      case "!" => !operand().asInstanceOf[Boolean]
      case "|" => operand()
      case _ => error("unknown unary operation " + op)
    }
    def sql = op match {
      case "-" => "-" + operand.sql
      case "!" =>
        (if (operand.exprType == classOf[SelectExpr]) "not exists " else "not ") +
          operand.sql
      case _ => error("unknown unary operation " + op)
    }
    override def exprType: Class[_] = if ("-" == op) operand.exprType else classOf[ConstExpr]
  }

  class BinExpr(val op: String, val lop: Expr, val rop: Expr) extends BaseExpr {
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
          QueryBuilder.this.bindVariables, env, QueryBuilder.this.allCols)
        case "++" => org.tresql.Query.select(sql, selCols(lop),
          QueryBuilder.this.bindVariables, env, QueryBuilder.this.allCols)
        case "+" => if (exprType == classOf[SelectExpr])
          org.tresql.Query.select(sql, selCols(lop), QueryBuilder.this.bindVariables, env,
            QueryBuilder.this.allCols)
        else lop + rop
        case "-" => if (exprType == classOf[SelectExpr])
          org.tresql.Query.select(sql, selCols(lop), QueryBuilder.this.bindVariables, env,
            QueryBuilder.this.allCols)
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
    def sql = op match {
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
    def sql = name + (params map (_.sql)).mkString("(", ",", ")")
    override def toString = name + (params map (_.toString)).mkString("(", ",", ")")
  }

  class ArrExpr(val elements: List[Expr]) extends BaseExpr {
    override def apply() = elements map (_())
    def sql = elements map { _.sql } mkString ("(", ", ", ")")
    override def toString = elements map { _.toString } mkString ("[", ", ", "]")
  }

  class SelectExpr(val tables: List[Table], val filter: List[Expr], val cols: List[ColExpr],
    val distinct: Boolean, val group: Expr, val order: List[Expr],
    offset: Expr, limit: Expr) extends BaseExpr {
    override def apply() = {
      org.tresql.Query.select(sql, cols, QueryBuilder.this.bindVariables, env,
        QueryBuilder.this.allCols)
    }
    lazy val sql = "select " + (if (distinct) "distinct " else "") +
      (if (cols == null) "*" else sqlCols) + " from " + join +
      (if (filter == null) "" else " where " + where) +
      (if (group == null) "" else " group by " + group.sql) +
      (if (order == null) "" else " order by " + (order map (_.sql)).mkString(", ")) +
      (if (offset == null) "" else " offset " + offset.sql) +
      (if (limit == null) "" else " limit " + limit.sql)
    def sqlCols = cols.filter(!_.separateQuery).map(_.sql).mkString(",")
    def join = tables match {
      case t :: Nil => t.sqlName
      case t :: l =>
        var pt = t; t.sqlName + " " +
          (l map { ct => val v = pt; pt = ct; ct.sqlJoin(v) }).mkString(" ")
      case _ => error("Knipis")
    }
    def where = filter match {
      case (c @ ConstExpr(x)) :: Nil => tables(0).aliasOrName + "." +
        env.table(tables(0).name).key.cols(0) + " = " + c.sql
      case (v @ VarExpr(x, _)) :: Nil => tables(0).aliasOrName + "." +
        env.table(tables(0).name).key.cols(0) + " = " + v.sql
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
  class Table(val table: Any, val alias: String, val join: List[Expr], val outerJoin: String) {
    def name = table match {
      case t: List[_] => t.mkString(".")
      case t: Expr if (t.exprType == classOf[SelectExpr]) => t.sql
    }
    def sqlName = name + (if (alias != null) " " + alias else "")
    def aliasOrName = if (alias != null) alias else name
    def sqlJoin(joinTable: Table) = {
      def sqlJoin(jl: List[Expr]) = (jl map { j =>
        outerJoinSql + " " + (j match {
          case i@IdentExpr(_, null) => sqlName + " on " + i.sql + " = " +
            aliasOrName + "." + env.table(name).key.cols.mkString
          //FIXME ident expr should not be here because alias does not refer to i
          case IdentExpr(i, a) => name + " " + a + " on " +
            i.mkString(".") + " = " + a + "." + env.table(name).key.cols.mkString
          case e => sqlName + " on " + (e match {
            case ConstExpr("/") => defaultJoin
            case e => e.sql
          })
        })
      }) mkString " "
      def outerJoinSql = (outerJoin match {
        case "l" => "left "
        case "r" => "right "
        case null => ""
      }) + "join"
      def defaultJoin = {
        val j = env.join(name, joinTable.name)
        (j._1 zip j._2 map { t =>
          aliasOrName + "." + t._1 + " = " +
            joinTable.aliasOrName + "." + t._2
        }) mkString " and "
      }
      join match {
        case Nil => ", " + sqlName
        case l => sqlJoin(l)
      }
    }
  }
  class ColExpr(val col: Expr, val alias: String) extends Expr {
    val separateQuery = QueryBuilder.this.separateQueryFlag
    if (!QueryBuilder.this.allCols) QueryBuilder.this.allCols = col.isInstanceOf[AllExpr]
    def aliasOrName = if (alias != null) alias else col match {
      case IdentExpr(n, a) => if (a == null) n(n.length - 1) else a
      case _ => null
    }
    def sql = col.sql + (if (alias != null) " " + alias else "")
    override def toString = col.toString + (if (alias != null) " " + alias else "")
  }
  case class IdentExpr(val name: List[String], val alias: String) extends Expr {
    def sql = name.mkString(".") + (if (alias == null) "" else " " + alias)
    def nameStr = name.mkString(".")
    def aliasOrName = if (alias != null) alias else nameStr
  }
  class Order(val ordExprs: (Null, List[Expr], Null), val asc: Boolean) extends Expr {
    def sql = (ordExprs._2 map (_.sql)).mkString(",") + (if (asc) " asc" else " desc") +
      (if (ordExprs._1 != null) " nulls first" else if (ordExprs._3 != null) " nulls last" else "")
  }
  class Group(val groupExprs: List[Expr], val having: Expr) extends Expr {
    def sql = (groupExprs map (_.sql)).mkString(",") +
      (if (having != null) " having " + having.sql else "")
  }

  class InsertExpr(table: IdentExpr, val cols: List[IdentExpr], val vals: List[Expr])
    extends DeleteExpr(table, null) {
    if (cols.length != vals.length) error("insert statement columns and values count differ: " +
      cols.length + "," + vals.length)
    override protected def _sql = "insert into " + table.sql +
      "(" + cols.map(_.sql).mkString(",") + ")" +
      " values (" + vals.map(_.sql).mkString(",") + ")"
  }
  class UpdateExpr(table: IdentExpr, filter: List[Expr], val cols: List[IdentExpr],
    val vals: List[Expr]) extends DeleteExpr(table, filter) {
    if (cols.length != vals.length) error("update statement columns and values count differ: " +
      cols.length + "," + vals.length)
    override protected def _sql = "update " + table.sql + " set " +
      (cols zip vals map { v => v._1.sql + " = " + v._2.sql }).mkString(",") +
      (if (filter == null) "" else " where " + where)
  }
  class DeleteExpr(val table: IdentExpr, val filter: List[Expr]) extends BaseExpr {
    override def apply() = org.tresql.Query.update(sql, QueryBuilder.this.bindVariables, env)
    protected def _sql = "delete from " + table.sql +
      (if (filter == null) "" else " where " + where)
    lazy val sql = _sql
    def where = filter match {
      case (c @ ConstExpr(x)) :: Nil => table.aliasOrName + "." +
        env.table(table.nameStr).key.cols(0) + " = " + c.sql
      case (v @ VarExpr(x, _)) :: Nil => table.aliasOrName + "." +
        env.table(table.nameStr).key.cols(0) + " = " + v.sql
      case f :: Nil => (if (f.exprType == classOf[SelectExpr]) "exists " else "") + f.sql
      case l => table.aliasOrName + "." + env.table(table.nameStr).key.cols(0) + " in(" +
        (l map { _.sql }).mkString(",") + ")"
    }
  }

  class BracesExpr(val expr: Expr) extends BaseExpr {
    override def apply() = expr()
    def sql = "(" + expr.sql + ")"
    override def exprType = expr.exprType
  }

  abstract class BaseExpr extends Expr {
    override def apply(params: Map[String, Any]): Any = {
      env update params
      apply()
    }
    override def close = env.closeStatement
    override def builder = QueryBuilder.this
  }

  //DML statements are defined outsided buildInternal method since they are called from other QueryBuilder
  private def buildInsert(table: List[String], cols: List[Col], vals: List[Any]) = {
    new InsertExpr(new IdentExpr(table, null), cols map { c =>
      new IdentExpr(c.col.asInstanceOf[Obj].obj.asInstanceOf[Ident].ident, null)
    }, vals map { buildInternal(_, VALUES_CTX) })
  }
  private def buildUpdate(table: List[String], filter: Arr, cols: List[Col], vals: List[Any]) = {
    new UpdateExpr(new IdentExpr(table, null), if (filter != null)
      filter.elements map { buildInternal(_, WHERE_CTX) } else null,
      cols map { c => new IdentExpr(c.col.asInstanceOf[Obj].obj.asInstanceOf[Ident].ident, null) },
      vals map { buildInternal(_, VALUES_CTX) })
  }
  private def buildDelete(table: List[String], filter: Arr) = {
    new DeleteExpr(new IdentExpr(table, null),
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
    def buildTable(t: Obj) = t match {
      case Obj(Ident(i), a, j, o) => new Table(i, a, buildJoin(j), o)
      case Obj(q, a, j, o) => new Table(buildInternal(q, TABLE_CTX), a, buildJoin(j), o)
    }

    def buildJoin(j: Any) = j match {
      case l: List[_] => l map { buildInternal(_, JOIN_CTX) }
      case e => buildInternal(e, JOIN_CTX) :: Nil
    }

    def buildIdent(c: Obj) = c match {
      case Obj(Ident(i), a, _, _) => new IdentExpr(i, a)
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
        case BinOp("+", Query(Obj(Ident(t), _, null, null) :: Nil, null, c @ (Col(Obj(Ident(_), _,
          null, null), _) :: l), _, null, null, _, _), Arr(v)) => parseCtx match {
          case ROOT_CTX => {
            val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
            val ex = b.buildInsert(t, c, v)
            this.bindIdx = b.bindIdx; ex
          }
          case _ => buildInsert(t, c, v)
        }
        //update
        case BinOp("=", Query(Obj(Ident(t), _, null, null) :: Nil, f, c @ (Col(Obj(Ident(_), _,
          null, null), _) :: l), _, null, null, _, _), Arr(v)) => parseCtx match {
          case ROOT_CTX => {
            val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
            val ex = b.buildUpdate(t, f, c, v)
            this.bindIdx = b.bindIdx; ex
          }
          case _ => buildUpdate(t, f, c, v)
        }
        //delete
        case BinOp("-", Obj(Ident(t), _, null, null), f @ Arr(_)) => parseCtx match {
          case ROOT_CTX => {
            val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
            val ex = b.buildDelete(t, f)
            this.bindIdx = b.bindIdx; ex
          }
          case _ => buildDelete(t, f)
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
        case Arr(l: List[_]) => new ArrExpr(l map { buildInternal(_, parseCtx) })
        case Variable("?", o) => this.bindIdx += 1; new VarExpr(this.bindIdx.toString, o)
        case Variable(n, o) => if (!env.reusableExpr && o && !(env contains n)) null else new VarExpr(n, o)
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

  private def build(parsedExpr: Any): Expr = {
    buildInternal(parsedExpr)
  }

  private def build(ex: String): Expr = {
    parseAll(ex) match {
      case Success(r, _) => build(r)
      case x => error(x.toString)
    }
  }

  override def toString = "QueryBuilder: " + queryDepth

}

object QueryBuilder {
  import metadata._

  def apply(parsedExpr: Any, env: Env): Expr = {
    new QueryBuilder(env).build(parsedExpr)
  }

  def apply(ex: String, env: Env): Expr = {
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
  def sql: String
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
  def builder: QueryBuilder = error("Must be implemented in subclass")
  override def toString = sql
}