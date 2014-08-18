package org.tresql

import sys._
import scala.util.Try
import QueryParser._

class QueryBuilder private (val env: Env, queryDepth: Int, private var bindIdx: Int)
  extends EnvProvider with Transformer with Typer {

  val ROOT_CTX = "ROOT"
  val QUERY_CTX = "QUERY"
  val FROM_CTX ="FROM_CTX"
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

  //parsed expression from which executable expr is built
  private var exp: Any = null
  
  //used for non flat updates, i.e. hierarhical object.
  private val childUpdates = scala.collection.mutable.ListBuffer[(Expr, String)]()

  //context stack as buildInternal method is called
  protected var ctxStack = List[String]()
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
  private var hasHiddenCols = false
  //table defs from Typer. This is used for establishing relationships between tables and hierarchical queries
  protected var tableDefs: List[Def] = Nil
  //table alias and key column(s) (multiple in the case of composite primary key) to be added as
  //hidden columns for query built by this builder in order to establish relation ship with query
  //built by child builder
  private var joinsWithChildren: Set[(String, List[String])] = Set()
  lazy val joinsWithChildrenColExprs = for {
    tc <- { if (this.joinsWithChildren.size > 0) this.hasHiddenCols = true; this.joinsWithChildren }
    c <- tc._2
  } yield (ColExpr(IdentExpr(List(tc._1, c)), tc._1 + "_" + c + "_", null, Some(false), true))

  case class ConstExpr(value: Any) extends BaseExpr {
    override def apply() = value
    def defaultSQL = value match {
      case v: Int => v.toString
      case v: Number => v.toString
      case v: String => "'" + v + "'"
      case v: Boolean => v.toString
      case null => "null"
      case x => String.valueOf(x)
    }
  }

  case class AllExpr() extends PrimitiveExpr {
    def defaultSQL = "*"
  }
  case class IdentAllExpr(name: List[String]) extends PrimitiveExpr {
    QueryBuilder.this.identAll = true
    def defaultSQL = name.mkString(".") + ".*"
  }

  case class VarExpr(name: String, typ: String, opt: Boolean) extends BaseExpr {
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
          case _: Array[Byte] => "?"
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

  case class IdExpr(seqName: String) extends BaseExpr {
    override def apply() = idFromEnv getOrElse env.nextId(seqName)
    private def idFromEnv = env.tableOption(seqName).map(_.key.cols).filter(_.size == 1).map(_(0))
        .filter(env contains).flatMap(key => Option(env(key))).map { id =>
          //if primary key is set as an environment variable use it instead of sequence
          env.currId(seqName, id)
          id
        }
    var binded = false
    def defaultSQL = {
      if (!binded) { QueryBuilder.this._bindVariables += this; binded = true }
      "?"
    }
    override def toString = s"#$seqName = ${idFromEnv getOrElse "<sequence next value>"}"
  }

  case class IdRefExpr(seqName: String) extends BaseExpr {
    override def apply() = env.currId(seqName)
    var binded = false
    def defaultSQL = {
      if (!binded) { QueryBuilder.this._bindVariables += this; binded = true }
      "?"
    }
    override def toString = s":#$seqName = ${this()}"
  }

  case class ResExpr(nr: Int, col: Any) extends PrimitiveExpr {
    override def apply() = env(nr) match {
      case null => error("Ancestor result with number " + nr + " not found for expression " + this)
      case r => col match {
        case c: Ident => r(c.ident.mkString("."))
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
    override def toString = s"$nr($col) = ${Try(this()).getOrElse("value not available")}"
  }

  case class AssignExpr(variable: String, value: Expr) extends BaseExpr {
    //add variable to environment so that variable is found when referenced in further expressions
    env(variable) = null
    override def apply() = {
      env(variable) = value()
      env(variable)
    }
    def defaultSQL = error("Cannot construct sql statement from assignment expression")
    override def toString = variable + " = " + value
  }

  case class UnExpr(op: String, operand: Expr) extends BaseExpr {
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

  case class InExpr(lop: Expr, rop: List[Expr], not: Boolean) extends BaseExpr {
    override def apply = if (not) lop notIn rop else lop in rop
    def defaultSQL = lop.sql + (if (not) " not" else "") + rop.map(_.sql).mkString(" in(", ", ", ")")
    override def exprType = classOf[ConstExpr]
  }

  case class BinExpr(op: String, lop: Expr, rop: Expr) extends BaseExpr {
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
          QueryBuilder.this.identAll, QueryBuilder.this.hasHiddenCols)
        case "++" => org.tresql.Query.sel(sql, selCols(lop),
          QueryBuilder.this.bindVariables, env, QueryBuilder.this.allCols,
          QueryBuilder.this.identAll, QueryBuilder.this.hasHiddenCols)
        case "+" => if (exprType == classOf[SelectExpr])
          org.tresql.Query.sel(sql, selCols(lop), QueryBuilder.this.bindVariables, env,
            QueryBuilder.this.allCols, QueryBuilder.this.identAll, QueryBuilder.this.hasHiddenCols)
        else lop + rop
        case "-" => if (exprType == classOf[SelectExpr])
          org.tresql.Query.sel(sql, selCols(lop), QueryBuilder.this.bindVariables, env,
            QueryBuilder.this.allCols, QueryBuilder.this.identAll, QueryBuilder.this.hasHiddenCols)
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

  case class FunExpr(name: String, params: List[Expr], distinct: Boolean) extends BaseExpr {
    override def apply() = {
      val p = params map (_())
      val ts = p map {
        case null => classOf[Any]
        case x => x.getClass
      }
      if (Env.isDefined(name)) {
        try {
          Env log ts.mkString("Trying to call locally defined function: " + name + "(", ", ", ")")
          Env.functions.map(f => f.getClass.getMethod(name, ts: _*).invoke(
            f, p.asInstanceOf[List[Object]]: _*)).get
        } catch {
          case ex: NoSuchMethodException => {
            Env.functions.flatMap(f => f.getClass.getMethods.filter(m =>
              m.getName == name && (m.getParameterTypes match {
                case Array(par) => par.isAssignableFrom(classOf[Seq[_]])
                case _ => false
              })).headOption.map(_.invoke(f, List(p).asInstanceOf[List[Object]]: _*)).orElse(Some(
              org.tresql.Query.call("{call " + sql + "}", QueryBuilder.this.bindVariables, env)))).get
          }
        }
      } else org.tresql.Query.call("{call " + sql + "}", QueryBuilder.this.bindVariables, env)
    }
    def defaultSQL = name + (params map (_.sql))
      .mkString("(" + (if (distinct) "distinct " else ""), ",", ")")
    override def toString = name + (params map (_.toString)).mkString("(", ",", ")")
  }

  case class ExternalFunExpr(name: String, params: List[Expr],
    method: java.lang.reflect.Method) extends BaseExpr {
    override def apply() = {
      val p = params map (_())
      val parTypes = method.getParameterTypes
      val varargs = parTypes.size == 1 && parTypes(0).isAssignableFrom(classOf[Seq[_]])
      if (parTypes.size != params.size && !varargs) error("Wrong parameter count for method "
        + method + " " + parTypes.size + " != " + params.size)
      Env.functions.map(method.invoke(_, (if (varargs) List(p) else p).asInstanceOf[List[Object]]: _*))
        .getOrElse("Functions not defined in Env!")
    }
    def defaultSQL = error("Method not implemented for external function")
    override def toString = name + (params map (_.toString)).mkString("(", ",", ")")
  }
  
  case class RecursiveExpr(exp: Any) extends BaseExpr {
    if (queryDepth >= Env.recursive_stack_dept)
      error(s"Recursive execution stack depth $queryDepth exceeded, check for loops in data or increase Env.recursive_stack_dept setting.") 
    val qBuilder = new QueryBuilder(new Env(QueryBuilder.this, QueryBuilder.this.env.reusableExpr),
        queryDepth + 1, -1)
    qBuilder.exp = QueryBuilder.this.exp
    lazy val expr: Expr = qBuilder.buildInternal(exp, QUERY_CTX)
    override def apply() = expr()
    def defaultSQL = expr sql
  }
  
  case class ArrExpr(elements: List[Expr]) extends BaseExpr {
    override def apply() = elements map (_())
    def defaultSQL = elements map { _.sql } mkString ("(", ", ", ")")
    override def toString = elements map { _.toString } mkString ("[", ", ", "]")
  }

  case class SelectExpr(tables: List[Table], filter: Expr, cols: List[ColExpr],
    distinct: Boolean, group: Expr, order: Expr,
    offset: Expr, limit: Expr, aliases: Map[String, Table], parentJoin: Option[Expr]) extends BaseExpr {
    override def apply() = {
      org.tresql.Query.sel(sql, cols, QueryBuilder.this.bindVariables, env,
        QueryBuilder.this.allCols, QueryBuilder.this.identAll, QueryBuilder.this.hasHiddenCols)
    }
    lazy val defaultSQL = "select " + (if (distinct) "distinct " else "") +
      (if (cols == null) "*" else sqlCols) + " from " + tables.head.sqlName + join(tables) +
      //(filter map where).getOrElse("")
      Option(where).map(" where " + _).getOrElse("") +
      (if (group == null) "" else " group by " + group.sql) +
      (if (order == null) "" else " order by " + order.sql) +
      (if (offset == null) "" else " offset " + offset.sql) +
      (if (limit == null) "" else " limit " + limit.sql)
    def sqlCols = cols.withFilter(!_.separateQuery).map(_.sql).mkString(", ")
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
    def where = {
      val fsql = Option(filter).map(
          e => (if (e.exprType == classOf[SelectExpr]) "exists " else "") + e.sql).orNull
      parentJoin.map(e => Option(fsql).map("(" + _ + ") and " + e.sql)
        .getOrElse(e.sql)).getOrElse(fsql)
    }
    val queryDepth = QueryBuilder.this.queryDepth
    override def toString = sql + " (" + QueryBuilder.this + ")\n" +
      (if (cols != null) cols.filter(_.separateQuery).map {
        "    " * (queryDepth + 1) + _.toString
      }.mkString
      else "")
  }
  case class Table(table: Expr, alias: String, join: TableJoin, outerJoin: String, nullable: Boolean)
  extends PrimitiveExpr {
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
      def fkJoin(i: IdentExpr) = sqlName + " on " + (if (i.name.size < 2 && joinTable.alias != null)
        i.copy(name = joinTable.alias :: i.name) else i).sql +
        " = " + aliasOrName + "." + env.table(name).key.cols.mkString
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
        case TableJoin(false, ArrExpr(Nil) | null, _, _) => ", " + sqlName
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
    override def defaultSQL = table.sql + Option(alias).mkString
  }
  case class TableJoin(default: Boolean, expr: Expr, noJoin: Boolean,
    defaultJoinCols: (List[String], List[String])) extends PrimitiveExpr {
    def defaultSQL = error("Not implemented")
    override def toString = "TableJoin(\ndefault: " + default + "\nexpr: " + expr +
      "\nno join flag: " + noJoin + ")\n"
  }
  case class ColExpr(col: Expr, alias: String, typ: String,
      sepQuery: Option[Boolean] = None, hidden: Boolean = false)
    extends PrimitiveExpr {
    val separateQuery = sepQuery.getOrElse(QueryBuilder.this.separateQueryFlag)
    if (!QueryBuilder.this.allCols) QueryBuilder.this.allCols = col.isInstanceOf[AllExpr]
    def defaultSQL = col.sql + (if (alias != null) " " + alias else "")
    def name = if (alias != null) alias.stripPrefix("\"").stripSuffix("\"") else col match {
      case IdentExpr(n) => n(n.length - 1)
      case _ => null
    }
    override def toString = col.toString + (if (alias != null) " " + alias else "") + ", hidden = " + hidden
  }
  case class HiddenColRefExpr(expr: Expr, resType: Class[_]) extends PrimitiveExpr {
    override def apply() = {
      val (res, idx) = (env(0), env(this))
      if (resType == classOf[Int]) res.int(idx)
      else if (resType == classOf[Long]) res.long(idx)
      else if (resType == classOf[Double]) res.double(idx)
      else if (resType == classOf[Boolean]) res.boolean(idx)
      else if (resType == classOf[BigDecimal]) res.bigdecimal(idx)
      else if (resType == classOf[String]) res.string(idx)
      else if (resType == classOf[java.util.Date]) res.timestamp(idx)
      else if (resType == classOf[java.sql.Date]) res.date(idx)
      else if (resType == classOf[java.sql.Timestamp]) res.timestamp(idx)
      else if (resType == classOf[java.lang.Integer]) res.jInt(idx)
      else if (resType == classOf[java.lang.Long]) res.jLong(idx)
      else if (resType == classOf[java.lang.Double]) res.jDouble(idx)
      else if (resType == classOf[java.lang.Boolean]) res.jBoolean(idx)
      else if (resType == classOf[Array[Byte]]) res.bytes(idx)
      else if (resType == classOf[java.io.InputStream]) res.stream(idx)
      else if (resType == classOf[java.math.BigDecimal]) res.bigdecimal(idx) match {
        case null => null
        case bd => bd.bigDecimal
      } else res(idx)
    }
    override def defaultSQL = expr.sql
    override def toString = expr + ":" + resType
  }  
  case class IdentExpr(name: List[String]) extends PrimitiveExpr {
    def defaultSQL = name.mkString(".")
  }
  case class Order(ordExprs: List[(Expr, Expr, Expr)]) extends PrimitiveExpr {
    def defaultSQL = ordExprs.map(o => (o._2 match {
      case UnExpr("~", e) => e.sql + " desc"
      case e => e.sql + " asc"
    }) + (if (o._1 != null) " nulls first" else if (o._3 != null) " nulls last" else "")).mkString(", ")
  }
  case class Group(groupExprs: List[Expr], having: Expr) extends PrimitiveExpr {
    def defaultSQL = (groupExprs map (_.sql)).mkString(",") +
      (if (having != null) " having " + having.sql else "")
  }

  class InsertExpr(table: IdentExpr, alias: String, val cols: List[Expr], val vals: Expr)
    extends DeleteExpr(table, alias, null) {
    //include env.currId(idExpr.seqName) in result if vals contains IdExpr
    override def apply() = {
      val r = super.apply() 
      vals match {
        case ValuesExpr(ArrExpr(v) :: _) => v.find(_.isInstanceOf[IdExpr])
          .map(e => env.currId(e.asInstanceOf[IdExpr].seqName)).map((r, _)).getOrElse(r)
        case _ => r
      }
    }
    override protected def _sql = "insert into " + table.sql + (if (alias == null) "" else " " + alias) +
      " (" + cols.map(_.sql).mkString(", ") + ")" + " " + vals.sql
  }
  case class ValuesExpr(vals: List[Expr]) extends PrimitiveExpr {
    def defaultSQL = vals map (_.sql) mkString("values " + (if (vals.size > 1) "(" else ""), ",",
        if (vals.size > 1) ")" else "")
  }
  class UpdateExpr(table: IdentExpr, alias: String, filter: List[Expr],
      val cols: List[Expr], val vals: Expr)
  extends DeleteExpr(table, alias, filter) {
    override protected def _sql = "update " + table.sql + (if (alias == null) "" else " " + alias) +
      " set " + (vals match {
        case ValuesExpr(v) => (cols zip v map { v => v._1.sql + " = " + v._2.sql }).mkString(", ")
        case q: SelectExpr => cols.map(_.sql).mkString("(", ", ", ")") + " = " + "(" + q.sql + ")"
        case x => error("Knipis: " + x)
      }) + (if (filter == null) "" else " where " + where)
  }
  case class DeleteExpr(table: IdentExpr, alias: String, filter: List[Expr]) extends BaseExpr {
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
      case (v @ VarExpr(x, _, _)) :: Nil => Option(alias).getOrElse(table.sql) + "." +
        env.table(table.sql).key.cols(0) + " = " + v.sql
      case f :: Nil => (if (f.exprType == classOf[SelectExpr]) "exists " else "") + f.sql
      case l => Option(alias).getOrElse(table.sql) + "." + env.table(table.sql).key.cols(0) + " in(" +
        (l map { _.sql }).mkString(",") + ")"
    }
  }

  case class BracesExpr(expr: Expr) extends BaseExpr {
    override def apply() = expr()
    def defaultSQL = "(" + expr.sql + ")"
    override def exprType = expr.exprType
  }
  
  //sql helper expressions to enable advanced syntax. these expression are expected to be
  //create with the help of macros
  case class SQLExpr(sqlSnippet: String, bindVars: List[VarExpr]) extends PrimitiveExpr {
    def defaultSQL = {
      bindVars foreach(_.sql)
      sqlSnippet
    }
  }
  case class SQLConcatExpr(delimiter: String = " ", expr1: Expr, expr2: Expr) extends BaseExpr {
    private def findSQL(expr: Expr): Option[QueryBuilder#SelectExpr] = expr match {
      case e: QueryBuilder#SQLConcatExpr => findSQL(e.expr2) orElse findSQL(e.expr1)
      case e: QueryBuilder#SelectExpr => Some(e)
      case _ => None
    }
    val sqlExpr = findSQL(this) getOrElse
      error(s"Unable to execute SQLConcatExpr $this because it does not contain SelectExpr")
    override def apply() =
      org.tresql.Query.sel(sql, sqlExpr.cols, QueryBuilder.this.bindVariables, env,
        QueryBuilder.this.allCols, QueryBuilder.this.identAll, QueryBuilder.this.hasHiddenCols)
    def defaultSQL = expr1.sql + delimiter + expr2.sql
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

  //default or fk shortcut join with child.
  //return value in form: (child query table col(s) -> parent query cols(s) alias(es))
  private def joinWithChild(childTable: String,
    refCol: Option[String]): Option[(List[String], List[String])] =
    (refCol map { ref =>
      val refTable = env.table(childTable).refTable.getOrElse(metadata.Ref(List(ref)),
        error("No referenced table found for table: " + childTable + ". Reference: " + ref))
      List(ref) -> env.table(refTable).key.cols -> findAliasByName(refTable)
        .getOrElse(error("Unable to find relationship between table " + childTable +
          ", reference column: " + ref + " and tables: " + this.tableDefs))
    } orElse this.findJoin(childTable)) match {
      case None => None
      case o @ Some(x) =>
        this.joinsWithChildren += (x._2 -> x._1._2)
        o.map(j => j._1._1 -> j._1._2.map(j._2 + "_" + _ + "_"))
    }
  //default or fk shortcut join with parent
  private def joinWithParent(childTable: String, refCol: Option[String] = None) = env.provider.flatMap {
    case b: QueryBuilder => b.joinWithChild(childTable, refCol)
    case x => None
  }
  //join with ancestor query
  private def joinWithAncestor(ancestorTableAlias: String,
    ancestorTableCol: String): Option[ResExpr] =
    if (!this.tableDefs.exists(_.alias == ancestorTableAlias))
      joinWithAncestor(ancestorTableAlias, ancestorTableCol, 1)
        .map(ResExpr(_, ancestorTableAlias + "_" + ancestorTableCol + "_"))
    else None
  private def joinWithAncestor(ancestorTableAlias: String, ancestorTableCol: String,
      resIdx: Int): Option[Int] = env.provider.flatMap {
    case b: QueryBuilder =>
      b.joinWithDescendant(ancestorTableAlias, ancestorTableCol).map(x=> resIdx).orElse(
          b.joinWithAncestor(ancestorTableAlias, ancestorTableCol, resIdx + 1)) 
    case _ => None
  }
  private def joinWithDescendant(tableAlias: String, tableCol: String) =
    this.tableDefs.find(tableAlias == _.alias).map(x=> {
      this.joinsWithChildren += (tableAlias -> List(tableCol))
      x
    })

  //DML statements are defined outsided buildInternal method since they are called from other QueryBuilder
  private def buildInsert(table: Ident, alias: String, cols: List[Col], vals: Any) = {
    new InsertExpr(IdentExpr(table.ident), alias, Option(cols) map (_ map (buildInternal(_, COL_CTX)) filter {
      case x @ ColExpr(IdentExpr(_), _, _, _, _) => true
      case e: ColExpr => this.childUpdates += { (e.col, e.name) }; false
    }) getOrElse this.table(table).cols.map(c => IdentExpr(List(c.name))), vals match {
      case arr: List[_] => ValuesExpr(arr map {
        buildInternal(_, VALUES_CTX) match {
          case ArrExpr(l) => ArrExpr(patchVals(table, cols, l))
          case null => ArrExpr(patchVals(table, cols, Nil))
          case e => e
        }
      } match {
        case Nil => patchVals(table, cols, Nil)
        case l => l
      })
      case q => buildInternal(q, VALUES_CTX)
    })
  }
  private def buildUpdate(table: Ident, alias: String, filter: Arr, cols: List[Col], vals: Any) = {
    new UpdateExpr(IdentExpr(table.ident), alias, if (filter != null)
      filter.elements map { buildInternal(_, WHERE_CTX) } else null,
      Option(cols) map (_ map (buildInternal(_, COL_CTX)) filter {
        case ColExpr(IdentExpr(_), _, _, _, _) => true
        case e: ColExpr => this.childUpdates += { (e.col, e.name) }; false
      }) getOrElse this.table(table).cols.map(c => IdentExpr(List(c.name))),
      buildInternal(vals, VALUES_CTX) match {
        case v: ArrExpr => ValuesExpr(patchVals(table, cols, v.elements))
        case q: SelectExpr => q
        case null => ValuesExpr(patchVals(table, cols, Nil))
        case x => error("Knipis: " + x)
      })
  }
    
  private def buildDelete(table: Ident, alias: String, filter: Arr) = {
    new DeleteExpr(IdentExpr(table.ident), alias,
      if (filter != null) filter.elements map { buildInternal(_, WHERE_CTX) } else null)
  }
  private def table(t: Ident) = env.table(t.ident.mkString("."))
  private def patchVals(table: Ident, cols: List[Col], vals: List[Expr]) = {
    val diff = Option(cols).getOrElse(this.table(table).cols).size - vals.size
    val allExprIdx = vals.indexWhere(_.isInstanceOf[AllExpr])
    def v(i: Int) = buildInternal(Variable("?", null, false), VALUES_CTX)
    if (diff > 0 || allExprIdx != -1) allExprIdx match {
      case -1 if vals.size == 0 => 1 to diff map v toList //empty value clause
      case -1 => vals //perhaps hierarchical update
      case i => vals.patch(i, 0 to diff map v, 1)
    }
    else vals
  }
  
  private def buildInternal(parsedExpr: Any, parseCtx: String = ROOT_CTX): Expr = {
    def buildSelect(q: QueryParser.Query) = {
      val tablesAndAliases = buildTables(q.tables)
      if (ctxStack.head == QUERY_CTX && this.tableDefs == Nil) this.tableDefs = defs(tablesAndAliases._1)
      val filter = if (q.filter == null) null else buildFilter(tablesAndAliases._1.last, q.filter.filters)
      val cols = buildCols(q.cols)
      val distinct = q.distinct
      val group = buildInternal(q.group, GROUP_CTX)
      val order = buildInternal(q.order, ORD_CTX)
      val offset = buildInternal(q.offset, LIMIT_CTX)
      val limit = buildInternal(q.limit, LIMIT_CTX)
      val aliases = tablesAndAliases._2
      //check whether parent query with at least one primitive table exists
      def hasParentQuery = env.provider.map {
        case b: QueryBuilder => b.tableDefs != Nil
        case _ => false
      } getOrElse (false)
      //establish link between ancestors
      val parentJoin =
        if (QueryBuilder.this.ctxStack.headOption.orNull != QUERY_CTX || !hasParentQuery) None else {
          def parentChildJoinExpr(table: Table, qname: List[String], refCol: Option[String] = None) = {
            def exp(childCol: String, parentCol: String) = BinExpr("=",
              IdentExpr(List(table.aliasOrName, childCol)), ResExpr(1, parentCol))
            def exps(cols: List[(String, String)]): Expr = cols match {
              case c :: Nil => exp(c._1, c._2)
              case c :: l => BinExpr("&", exp(c._1, c._2), exps(l))
            }
            joinWithParent(qname mkString ".", refCol).map(t => exps(t._1 zip t._2))
          }
          tablesAndAliases._1.headOption.flatMap {
            //default join
            case tb @ Table(x, _, null, _, _) => x match {
              case IdentExpr(t) => parentChildJoinExpr(tb, t)
              case _ => error("At the moment default join with parent query cannot be performed on table: " + x)
            }
            case tb @ Table(x, _, TableJoin(true, null, _, _), _, _) => x match {
              case IdentExpr(t) => parentChildJoinExpr(tb, t)
              case _ => error("At the moment default join with parent query cannot be performed on table: " + x)
            }
            //foreign key join shortcut syntax
            case tb @ Table(x, _, TableJoin(false, IdentExpr(fk), _, _), _, _) => x match {
              case IdentExpr(t) => parentChildJoinExpr(tb, t, fk.lastOption)
              case _ => error("At the moment foreign key shortcut join with parent query cannot be performed on table: " + x)
            }
            //product join, i.e. no join
            case Table(_, _, TableJoin(false, ArrExpr(Nil) | null, _, _), _, _) => None
            //ancestor join
            //transform ancestor reference: replace IdentExpr referencing parent queries with ResExpr
            case tb @ Table(_, _, TableJoin(false, e, _, _), _, _) if e != null =>
              Some(transform(e, {
                case ie @ IdentExpr(List(tab, col)) =>
                  joinWithAncestor(tab, col).getOrElse(ie)
              }))
          }
        }
      val sel = SelectExpr(tablesAndAliases._1, filter, cols, distinct, group, order, offset, limit,
        tablesAndAliases._2, parentJoin)
      //if select expression is subquery in other's expression where clause, has where clause itself
      //but where clause was removed due to unbound optional variables remove subquery itself
      if (ctxStack.head == WHERE_CTX && q.filter.filters != Nil && sel.filter == null) null else sel
    }
    def buildSelectFromObj(o: Obj) =
      buildSelect(QueryParser.Query(List(o), null, null, false, null, null, null, null))
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
        (nt :: ts._1) -> 
         (if (nt.alias != null && !ts._2.contains(nt.alias)) ts._2 + (nt.alias -> nt) else ts._2)
      }) match {
        case (tables: List[Table], aliases: Map[String, Table]) => (tables.reverse, aliases)
      }
    }
    def buildTable(t: Obj) = Table(buildInternal(t.obj,
      if (ctxStack.head == QUERY_CTX) FROM_CTX else TABLE_CTX), t.alias,
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
    def buildCols(cols: List[Col]) = {
      //reset all cols flags
      this.identAll = false
      this.allCols = false
      if (cols == null)
        List(ColExpr(AllExpr(), null, null, Some(false)))
      else {
        val colExprs = cols.map(buildInternal(_, COL_CTX).asInstanceOf[ColExpr])
        var aliases = colExprs flatMap {
          case ColExpr(_, a, _, _, _) if a != null => List(a)
          case ColExpr(IdentExpr(i), null, _, _, _) => List(i.last)
          case _ => Nil
        } toSet
        var uid = 1
        @scala.annotation.tailrec def uniqueAlias(id: Int): String = {
          val a = "t" + id
          if (!(aliases contains a)) {
            aliases += a
            uid = id + 1
            a
          } else uniqueAlias(id + 1)
        }
        (if (colExprs.exists {
          case ColExpr(FunExpr(n, _, false), _, _, _, _) => Env.isDefined(n)
          case _ => false
        }) (colExprs flatMap { //external function found
          case ce @ ColExpr(FunExpr(n, pars, false), a, t, _, _) if Env.isDefined(n) && !ce.separateQuery =>
            val m = Env.functions.flatMap(_.getClass.getMethods.filter(m => m.getName == n && {
              val pt = m.getParameterTypes
              //parameter count must match or must be of variable count
              pt.size == pars.size || (pt.size == 1 && pt(0).isAssignableFrom(classOf[Seq[_]]))
            }).headOption).getOrElse(
              error("External function " + n + " with " + pars.size + " parameters not found"))
            QueryBuilder.this.hasHiddenCols = true
            val parameterTypes = (m.getParameterTypes match {
              case Array(l) if (l.isAssignableFrom(classOf[Seq[_]])) =>
                m.getGenericParameterTypes()(0).asInstanceOf[java.lang.reflect.ParameterizedType]
                 .getActualTypeArguments()(0) match {
                  case c: Class[_] => Array(c) //vararg parameter type
                }
              case l => l
            }) toList
            val hiddenColPars = parameterTypes.padTo(pars.size,
                parameterTypes.headOption.orNull).zip(pars).map(tp => HiddenColRefExpr(tp._2, tp._1))
           ColExpr(ExternalFunExpr(n, hiddenColPars, m), a, t, Some(true)) ::
              hiddenColPars.map(ColExpr(_, uniqueAlias(uid), null, Some(ce.separateQuery), true))
          case e => List(e)
        }).groupBy(_.hidden) match { //put hidden columns at the end
          case m: Map[Boolean, ColExpr] => m(false) ++ m.getOrElse(true, Nil)
        }
        else colExprs) ++
          //for top level queries add hidden columns used in filters of descendant queries
          (if (ctxStack.headOption.orNull == QUERY_CTX) joinsWithChildrenColExprs else Nil)  
      }
    }
    def buildFilter(pkTable: Table, filterList: List[Arr]): Expr = {
      def transformExpr(e: Expr) = e match {
        case a @ ArrExpr(List(_: ConstExpr | _: VarExpr | _: ResExpr)) => BinExpr("=",
          IdentExpr(List(pkTable.aliasOrName, env.tableOption(pkTable.name)
            .getOrElse(error("Table not found in primary key search: " + pkTable.name))
            .key.cols.head)), a.elements.head)
        case a: ArrExpr if a.elements.size > 1 => InExpr(IdentExpr(List(pkTable.aliasOrName,
          env.table(pkTable.name).key.cols.head)), a.elements, false)
        case ArrExpr(List(f)) => f
        case null => null
      }
      def filterExpr(fl: List[Arr]): Expr = fl match {
        case Nil => null
        case a :: Nil => transformExpr(buildInternal(a, WHERE_CTX))
        case a :: t => transformExpr(buildInternal(a, WHERE_CTX)) match {
          case null => filterExpr(t) case x => filterExpr(t) match {
            case null => x case y => BinExpr("&", BracesExpr(x), BracesExpr(y))
          }
        }
      }
      filterExpr(filterList)
    }
    def maybeCallMacro(fun: FunExpr) = {
      if (Env isMacroDefined fun.name) {
        Env.macros.map(m => m.getClass.getMethods.filter(_.getName == fun.name).head
            .invoke(m, (this :: fun.params): _*)).get.asInstanceOf[Expr]
      } else fun
    }
    ctxStack ::= parseCtx
    try {
      parsedExpr match {
        case x: Number => ConstExpr(x)
        case x: String => ConstExpr(x)
        case x: Boolean => ConstExpr(x)
        case Null => ConstExpr(null)
        //variable assignment
        case BinOp("=", Variable(n, _, o), v) if (parseCtx == ROOT_CTX) =>
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
        //recursive child query
        case UnOp("|", join: Arr) =>
          this.separateQueryFlag = true
          RecursiveExpr(exp match {
            case q: QueryParser.Query =>
              val t = q.tables
              //copy join condition in the right place for parent child join calculation 
              q.copy(tables = t.head.copy(join = QueryParser.Join(false, join, false)) :: t.tail,
                  //drop first filter for recursive query, since first filter is used to obtain initial set
                  filter = q.filter.copy(filters = q.filter.filters drop 1))
          })
        //child query
        case UnOp("|", oper) =>
          val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth + 1, bindIdx)
          val ex = b.buildInternal(oper, QUERY_CTX)
          this.separateQueryFlag = true; this.bindIdx = b.bindIdx; ex
        case t: Obj => parseCtx match {
          case ROOT_CTX => //top level query (might be part of expression list) 
            val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
            val ex = b.buildInternal(t, QUERY_CTX); this.bindIdx = b.bindIdx; ex
          case QUERY_CTX => t match { //top level query
            case Obj(b @ Braces(_), _, _, _, _) => buildInternal(b, parseCtx) //unwrap braces expression
            case _ => buildSelectFromObj(t)
          }
          case FROM_CTX | TABLE_CTX => buildSelectFromObj(t) //table in from clause of top level query or in any other subquery
          case COL_CTX => buildColumnIdentOrBracesExpr(t)
          case _ => buildIdentOrBracesExpr(t)
        }
        case q: QueryParser.Query => parseCtx match {
          case ROOT_CTX =>
            val b = new QueryBuilder(new Env(this, this.env.reusableExpr), queryDepth, bindIdx)
            b.exp = exp
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
        case t: TerOp => buildInternal(t.content, parseCtx)
        case In(lop, rop, not) =>
          val l = buildInternal(lop, parseCtx)
          if (l == null) null else {
            val r = rop.map(buildInternal(_, parseCtx)).filter(_ != null)
            if (r.size == 0) null else InExpr(l, r, not)
          }
        case Fun(n, pl: List[_], d) =>
          val pars = pl map { buildInternal(_, FUN_CTX) }
          if (pars.exists(_ == null)) null else maybeCallMacro(FunExpr(n, pars, d))
        case Ident(i) => IdentExpr(i)
        case IdentAll(i) => IdentAllExpr(i.ident)
        case Arr(l: List[_]) => l map { buildInternal(_, parseCtx) } filter (_ != null) match {
          case al if al.size > 0 => ArrExpr(al) case _ => null 
        }
        case Variable("?", t, o) =>
          this.bindIdx += 1; VarExpr(this.bindIdx.toString, t, o)
        case Variable(n, t, o) =>
          if (!env.reusableExpr && o && !(env contains n)) null else VarExpr(n, t, o)
        case Id(seq) => IdExpr(seq)
        case IdRef(seq) => IdRefExpr(seq)
        case Res(r, c) => ResExpr(r, c)
        case Col(c, a, t) => 
          separateQueryFlag = false
          ColExpr(buildInternal(c, parseCtx), a, t)
        case Grp(cols, having) => Group(cols map { buildInternal(_, GROUP_CTX) },
          buildInternal(having, HAVING_CTX))
        case Ord(cols) => Order(cols map (c=> (buildInternal(c._1, parseCtx),
            buildInternal(c._2, parseCtx), buildInternal(c._3, parseCtx))))
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
    exp = parseExp(ex)
    buildInternal(exp)
  }

  private def build(ex: Exp): Expr = {
    exp = ex
    buildInternal(ex)
  }
      
  override def toString = "QueryBuilder: " + queryDepth
  
  //for debugging purposes
  def printBuilderChain: Unit = {
    println(s"$this; ${this.exp}")
    env.provider.map {
      case b: QueryBuilder => b.printBuilderChain
      case _ => 
    }
  }

}

object QueryBuilder {
  def apply(ex: String, env: Env = Env(Map(), true)): Expr = {
    new QueryBuilder(env).build(ex)
  }
  def apply(ex: Exp, env: Env): Expr = {
    new QueryBuilder(env).build(ex)
  }
}

sealed abstract class Expr extends (() => Any) with Ordered[Expr] {

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
        b += r1.toRow ++ (if (r2.hasNext) { r2.next; r2.toRow } else Nil)
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
  //overrides function toString method since it is of no use
  override def toString = defaultSQL
}
