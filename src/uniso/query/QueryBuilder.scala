package uniso.query

import scala.collection._

class QueryBuilder(private var _env:Env, private val queryDepth:Int,
        private var bindIdx:Int) extends EnvProvider {

    import QueryParser._

    val QUERY_CTX = "TABLE"
    val JOIN_CTX = "JOIN"
    val WHERE_CTX = "WHERE"
    val COL_CTX = "COL"
    val ORD_CTX = "ORD"
    val GROUP_CTX = "GROUP"
    val HAVING_CTX = "HAVING"

    implicit val parseCtx = QUERY_CTX

    def this(env:Env) = this(env, 0, 0)
    def this() = this(null)
    
    private var bindVariables:mutable.LinkedList[Expr] = mutable.LinkedList()
    private var unboundVarsFlag = false
    private var separateQueryFlag = false

    def update(env: Env) = this._env = env
    def env = _env

    case class ConstExpr(val value: Any) extends Expr {
        def apply() = value
        def sql = value match {
            case v: Number => v.toString
            case v: String => "'" + v + "'"
            case v: Boolean => v.toString
            case null => "null"
        }
    }

    case class AllExpr() extends Expr {
        def apply() = {}
        def sql = "*"
    }

    case class VarExpr(val name: String, val opt: Boolean) extends Expr {
        if (!QueryBuilder.this.unboundVarsFlag) QueryBuilder.this.unboundVarsFlag = 
            !(env contains name)
        def apply() = env(name)
        def sql = {QueryBuilder.this.bindVariables = QueryBuilder.this.bindVariables :+ this; "?"}
    }

    class ResExpr(val nr:Int, val col: Any) extends Expr {
        def apply() = col match {
            case c:String => env(nr)(c)
            case c:Int => env(nr)(c)
        }
        def sql = {QueryBuilder.this.bindVariables = QueryBuilder.this.bindVariables :+ this; "?"}
    }

    class UnExpr(val op: String, val operand: Expr) extends Expr {
        def apply() = op match {
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

    class BinExpr(val op: String, val lop: Expr, val rop: Expr) extends Expr {
        def apply() = op match {
            case "*" => lop * rop
            case "/" => lop / rop
            case "+" => lop + rop
            case "-" => lop - rop
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
        def sql = op match {
            case "*" => lop.sql + " * " + rop.sql
            case "/" => lop.sql + " / " + rop.sql
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
            case "~" => lop.sql + " like " + rop.sql
            case _ => error("unknown operation " + op)
        }
        override def exprType: Class[_] = if (("+"::"-"::"*"::"/"::Nil) exists(_ == op)) {
            if (lop.exprType == rop.exprType) lop.exprType else super.exprType
        } else classOf[ConstExpr]
    }

    //TODO extend function location beyond Functions object
    case class FunExpr(val name: String, val params: List[Expr]) extends Expr {
        def apply() = {
            val p = params map (_())
            val ts = p map (_.asInstanceOf[AnyRef].getClass)
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
                    else throw ex
                }
            }
        }
        def sql = name + (params map (_.sql)).mkString("(", ",", ")")
    }

    class ArrExpr(val elements: List[Expr]) extends Expr {
        def apply() = elements map (_())
        def sql = elements map { _.sql } mkString ("(", ", ", ")")
    }

    class SelectExpr(val tables: List[Table], val filter: List[Expr], val cols: List[ColExpr],
        group: Expr, order: List[Expr]) extends Expr {
        def apply() = {
            uniso.query.Query.select(sql, cols, bindVariables, env)
        }
        val sql = "select " + (if (cols == null) "*" else sqlCols) + " from " + join +
            (if (filter == null) "" else " where " + where) +
            (if (group == null) "" else " group by " + group.sql) +
            (if (order == null) "" else " order by " + (order map (_.sql)).mkString(" "))
        val bindVariables = QueryBuilder.this.bindVariables.toList
        def sqlCols = cols.filter(!_.separateQuery).map(_.sql).mkString(",")
        def join = tables match {
            case t :: Nil => t.sqlName
            case t :: l =>
                var pt = t; t.sqlName + " " +
                    (l map { ct => val v = pt; pt = ct; ct.sqlJoin(v) }).mkString(" ")
            case _ => error("Knipis")
        }
        def where = filter match {
            case (c@ConstExpr(x))::Nil => tables(0).aliasOrName + "." + 
                env.tbl(tables(0).name).key.cols(0) + " = " + c.sql
            case (v@VarExpr(x, _))::Nil => tables(0).aliasOrName + "." + 
                env.tbl(tables(0).name).key.cols(0) + " = " + v.sql
            case f::Nil => (if (f.exprType == classOf[SelectExpr]) "exists " else "") + f.sql
            case l => tables(0).aliasOrName + "." + env.tbl(tables(0).name).key.cols(0) + " in(" +
                (l map {_.sql}).mkString(",") + ")"
        }
        val queryDepth = QueryBuilder.this.queryDepth
        override def toString = sql + "\n" + (if (cols != null )cols.filter(_.separateQuery).map {
            "    " * (queryDepth + 1) + _.sql}.mkString else "")
    }
    class Table(val table: Any, val alias: String, val join: List[Expr], val outerJoin: String) {
        def name = table match {
            case t: List[_] => t.mkString(".")
            case t: Expr if (t.exprType == classOf[SelectExpr]) => t.sql
        }
        def sqlName = name + (if (alias != null) " " + alias else "")
        def aliasOrName = if (alias != null) alias else name
        def sqlJoin(joinTable: Table) = {
            def sqlJoin(jl: List[Expr]) = (jl map {j =>
                outerJoinSql + " " + (j match {
                    case IdentExpr(i, null) => sqlName + " on " + i.mkString(".") + " = " +
                        aliasOrName + "." + env.tbl(name).key.cols.mkString
                    case IdentExpr(i, a) => name + " " + a + " on " + 
                        i.mkString(".") + " = " + a + "." + env.tbl(name).key.cols.mkString
                    case e => sqlName + " on " + (e match {
                        case ConstExpr("/") => defaultJoin
                        case e => e.sql
                    })})
            }) mkString " "
            def outerJoinSql = (outerJoin match {
                case "l" => "left "
                case "r" => "right "
                case null => ""
            }) + "join" 
            def defaultJoin = {
                val j = env.join(name, joinTable.name)
                (j._1 zip j._2 map { t => aliasOrName + "." + t._1 + " = " + 
                    joinTable.aliasOrName + "." + t._2 }) mkString " and "
            }
            join match {
                case Nil => outerJoinSql + " " + sqlName
                case l => sqlJoin(l)
            }                    
        }
    }
    class ColExpr(val col: Expr, val alias: String) extends Expr {
        val separateQuery = QueryBuilder.this.separateQueryFlag 
        def apply = {}
        def aliasOrName = if (alias != null) alias else col match {
            case IdentExpr(n, _) => n(n.length - 1)
            case _ => null
        }
        def sql = col.sql + (if (alias != null) " \"" + alias + "\"" else "")
    }
    case class IdentExpr(val name: List[String], val alias: String) extends Expr {
        def apply() = {}
        def sql = name.mkString(".") + (if (alias == null) "" else " \"" + alias + "\"")
        def nameStr = name.mkString(".")
        def aliasOrName = if (alias != null) alias else nameStr
    }    
    class Order(val ordExprs: List[Expr], val asc: Boolean) extends Expr {
        def apply() = {}
        def sql = (ordExprs map (_.sql)).mkString(",") + (if (asc) " asc" else " desc")
    }
    class Group(val groupExprs: List[Expr], val having: Expr) extends Expr {
        def apply() = {}
        def sql = (groupExprs map (_.sql)).mkString(",") +
            (if (having != null) " having " + having.sql else "")
    }

    class InsertExpr(table:IdentExpr, val cols:List[IdentExpr], val vals:List[Expr])
        extends DeleteExpr(table, null) {
        if (cols.length != vals.length) error("insert statement columns and values count differ: " +
                cols.length + "," + vals.length)
         override protected def _sql = "insert into " + table.sql +
             "(" + cols.map(_.sql).mkString(",") + ")" +
             " values (" + vals.map(_.sql).mkString(",") + ")"
    }
    class UpdateExpr(table:IdentExpr, val cols:List[IdentExpr], val vals:List[Expr],
            filter: List[Expr]) extends DeleteExpr(table, filter) {
        if (cols.length != vals.length) error("update statement columns and values count differ: " +
                cols.length + "," + vals.length)
        override protected def _sql = "update " + table.sql + " set " +
            (cols zip vals map{v => v._1.sql + " = " + v._2.sql}).mkString(",") +
            (if (filter == null) "" else " where " + where)
    }
    class DeleteExpr(val table:IdentExpr, val filter: List[Expr]) extends Expr {
        def apply() = uniso.query.Query.update(sql, bindVariables, env)
        protected def _sql = "delete from " + table.sql +
            (if (filter == null) "" else " where " + where)
        val sql = _sql
        val bindVariables = QueryBuilder.this.bindVariables.toList
        def where = filter match {
            case (c@ConstExpr(x))::Nil => table.aliasOrName + "." + 
                env.tbl(table.nameStr).key.cols(0) + " = " + c.sql
            case (v@VarExpr(x, _))::Nil => table.aliasOrName + "." + 
                env.tbl(table.nameStr).key.cols(0) + " = " + v.sql
            case f::Nil => (if (f.exprType == classOf[SelectExpr]) "exists " else "") + f.sql
            case l => table.aliasOrName + "." + env.tbl(table.nameStr).key.cols(0) + " in(" +
                (l map {_.sql}).mkString(",") + ")"
        }          
    }
    
    class BracesExpr(val expr: Expr) extends Expr {
        def apply() = expr()
        def sql = "(" + expr.sql + ")"
        override def exprType = expr.exprType
    }

    private def buildInternal(parsedExpr: Any)(implicit parseCtx: String): Expr = {
        def buildSelect(q: Query) = new SelectExpr(q.tables map buildTable,
            if (q.filter != null) q.filter.elements map {buildInternal(_)(WHERE_CTX)} else null,
            if (q.cols != null) q.cols map {buildInternal(_)(COL_CTX).asInstanceOf[ColExpr]} else null,
            buildInternal(q.group), if (q.order != null) q.order map { buildInternal(_)(ORD_CTX) } 
                else null)

        def buildTable(t: Obj) = t match {
            case Obj(Ident(i), a, j, o) => new Table(i, a, buildJoin(j), o)
            case Obj(q, a, j, o) => new Table(buildInternal(q)(QUERY_CTX), a, buildJoin(j), o)
        }

        def buildJoin(j: Any) = j match {
            case l:List[_] => l map {buildInternal(_)(JOIN_CTX)}
            case e => buildInternal(e)(JOIN_CTX)::Nil
        }
        
        def buildIdent(c: Obj) = c match {
            case Obj(Ident(i), a, _, _) => new IdentExpr(i, a)
            case o => error("unsupported column definition at this place: " + o)
        }
        
        def buildInsert(table:List[String], cols:List[Col], vals:List[Any]) = {
            new InsertExpr(new IdentExpr(table, null), cols map {c => 
                    new IdentExpr(c.col.asInstanceOf[Obj].obj.asInstanceOf[Ident].ident, null)}, 
                vals map {buildInternal(_)(parseCtx)})            
        }         
        def buildUpdate(table:List[String], cols:List[Col], vals:List[Any], filter:Arr) = {
            new UpdateExpr(new IdentExpr(table, null), cols map {c => 
                    new IdentExpr(c.col.asInstanceOf[Obj].obj.asInstanceOf[Ident].ident, null)}, 
                vals map {buildInternal(_)(parseCtx)},
                if (filter != null) filter.elements map {buildInternal(_)(WHERE_CTX)} else null)            
        }
        def buildDelete(table:List[String], filter:Arr) = {
            new DeleteExpr(new IdentExpr(table, null),
                if (filter != null) filter.elements map {buildInternal(_)(WHERE_CTX)} else null)            
        }

        parsedExpr match {
            case x: Number => ConstExpr(x)
            case x: String => ConstExpr(x)
            case x: Boolean => ConstExpr(x)
            case Null() => ConstExpr(null)
            //insert
            case BinOp("+", Query(Obj(Ident(t), _, null, null)::Nil, null, c@(Col(Obj(Ident(_), _,
                null, null), _)::l), null, null), Arr(v)) => buildInsert(t, c, v)
            //update
            case BinOp("=", Query(Obj(Ident(t), _, null, null)::Nil, f, c@(Col(Obj(Ident(_), _,
                null, null), _)::l), null, null), Arr(v)) => buildUpdate(t, c, v, f)            
            //delete
            case BinOp("-", Obj(Ident(t), _, null, null), f@Arr(_)) => buildDelete(t, f)            
            case UnOp("|", oper) => {
                val b = new QueryBuilder(new Env(this), queryDepth + 1, bindIdx)
                val ex = b.buildInternal(oper)(parseCtx)
                this.separateQueryFlag = true; this.bindIdx = b.bindIdx; ex
            }
            case UnOp(op, oper) => {
                val o = buildInternal(oper)(parseCtx)
                if (!unboundVarsFlag) new UnExpr(op, o) else {unboundVarsFlag = false; null}
            }
            case BinOp(op, lop, rop) => {
                var l = buildInternal(lop)(parseCtx)
                if (unboundVarsFlag) {l = null; unboundVarsFlag = false}
                var r = buildInternal(rop)(parseCtx)
                if (unboundVarsFlag) {r = null; unboundVarsFlag = false}
                if (l != null && r != null) new BinExpr(op, l, r) else
                    if (op == "&" || op == "|") if (l != null) l else if (r != null) r else null
                    else null
            }
            case Fun(n, pl: List[_]) => new FunExpr(n, pl map { buildInternal(_)(parseCtx) })
            case Arr(l: List[_]) => new ArrExpr(l map { buildInternal(_)(parseCtx) })
            case Variable("?", o) => this.bindIdx += 1; new VarExpr(this.bindIdx.toString, o)
            case Variable(n, o) => new VarExpr(n, o)
            case Result(r, c) => new ResExpr(r, c)
            case t: Obj =>
                parseCtx match {
                    case QUERY_CTX => new SelectExpr(List(buildTable(t)), null, null, null, null)
                    case _ => buildIdent(t)
                }
            case Col(c, a) => {separateQueryFlag = false; new ColExpr(buildInternal(c)(parseCtx), a)}
            case Grp(cols, having) => new Group(cols map { buildInternal(_)(GROUP_CTX) }, 
                    buildInternal(having)(HAVING_CTX))
            case Ord(cols, asc) => new Order(cols map { buildInternal(_)(parseCtx) }, asc)
            case q: Query => buildSelect(q)
            case All() => AllExpr()
            case null => null
            case Braces(expr) => new BracesExpr(buildInternal(expr)(parseCtx))
            case x => ConstExpr(x)
        }

    }
    
    def build(parsedExpr: Any):Expr = {
        this.bindIdx = 0
        this.bindVariables = mutable.LinkedList()
        this.unboundVarsFlag = false
        this.separateQueryFlag = false
        buildInternal(parsedExpr)
    }
    
    def build(ex: String):Expr = {
        parseAll(expr, ex) match {
            case Success(r, _) => build(r)
            case x => error(x.toString)
        }
    }

}

object QueryBuilder {
    import metadata._
    def main(args: Array[String]) {
        args.length match {
            case 0 => println("usage: <string to evaluate>")
            case 1 => {
                println(new QueryBuilder().build(args(0)))
            }
            case n => {
                Env.metaData(JSONMetaData.fromFile(args(0)))
                println(new QueryBuilder(Env(args.drop(1).dropRight(1).grouped(2).map(a => (a(0),
                    a(1))).toMap)(null)).build(args(args.length - 1)).sql)

            }
        }
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
    def exprType: Class[_] = this.getClass
}