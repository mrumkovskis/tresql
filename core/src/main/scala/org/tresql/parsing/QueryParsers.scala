package org.tresql.parsing

import org.tresql.MacroResources

import scala.util.parsing.combinator.JavaTokenParsers

private[tresql] object QueryParsers {
  private[tresql] val simple_ident_regex = "^[_\\p{IsLatin}][_\\p{IsLatin}0-9]*$".r

  def any2tresql(any: Any): String = any match {
    case a: String => if (a.contains("'")) "\"" + a + "\"" else "'" + a + "'"
    case e: Exp @unchecked => e.tresql
    case null => "null"
    case l: List[_] => l map any2tresql mkString ", "
    case x => x.toString
  }
}

import QueryParsers.any2tresql

trait Exp {
  def tresql: String
}

trait DMLExp extends Exp {
  def table: Ident
  def alias: String
  def cols: List[Col]
  def filter: Arr
  def vals: Exp
  def returning: Option[Cols]
}

case class Const(value: Any) extends Exp {
  def tresql = any2tresql(value)
}
case class Sql(sql: String) extends Exp {
  def tresql = s"`$sql`"
}
case class Ident(ident: List[String]) extends Exp {
  def tresql = ident.mkString(".")
}
case class Variable(variable: String, members: List[String], opt: Boolean) extends Exp {
  def tresql = if (variable == "?") "?" else {
    def var_str(v: String) =
      (if (QueryParsers.simple_ident_regex.pattern.matcher(v).matches) v
      else if (v contains "'") "\"" + v + "\""
      else "'" + v + "'")
    ":" + var_str(variable) +
      (if (members == null | members == Nil) "" else "." + (members map var_str mkString ".")) +
      (if (opt) "?" else "")
  }
}
case class Id(name: String) extends Exp {
  def tresql = "#" + name
}
case class IdRef(name: String) extends Exp {
  def tresql = ":#" + name
}
case class Res(rNr: Int, col: Any) extends Exp {
  def tresql = ":" + rNr + "(" + any2tresql(col) + ")"
}
case class Cast(exp: Exp, typ: String) extends Exp {
  def tresql = exp.tresql + "::" +
    (if (QueryParsers.simple_ident_regex.pattern.matcher(typ).matches) typ else "'" + typ + "'")
}
case class UnOp(operation: String, operand: Exp) extends Exp {
  def tresql = operation + operand.tresql
}
case class Fun(
                name: String,
                parameters: List[Exp],
                distinct: Boolean,
                aggregateOrder: Option[Ord],
                aggregateWhere: Option[Exp]
              ) extends Exp {
  def tresql = name + "(" + (if (distinct) "# " else "") +
    ((parameters map any2tresql) mkString ", ") +
    s""")${aggregateOrder.map(o => " " + o.tresql).mkString}${
      aggregateWhere.map(e => s"[${e.tresql}]").mkString}"""
}

case class TableColDef(name: String, typ: Option[String])
case class FunAsTable(fun: Fun, cols: Option[List[TableColDef]], withOrdinality: Boolean) extends Exp {
  def tresql = fun.tresql
}

case class In(lop: Exp, rop: List[Exp], not: Boolean) extends Exp {
  def tresql = any2tresql(lop) + (if (not) " !" else " ") + rop.map(any2tresql).mkString(
    "in(", ", ", ")")
}
case class BinOp(op: String, lop: Exp, rop: Exp) extends Exp {
  def tresql = lop.tresql + " " + op + " " + rop.tresql
}
case class TerOp(lop: Exp, op1: String, mop: Exp, op2: String, rop: Exp) extends Exp {
  def content = BinOp("&", BinOp(op1, lop, mop), BinOp(op2, mop, rop))
  def tresql = s"${any2tresql(lop)} $op1 ${any2tresql(mop)} $op2 ${any2tresql(rop)}"
}

case class Join(default: Boolean, expr: Exp, noJoin: Boolean) extends Exp {
  def tresql = this match {
    case Join(_, _, true) => ";"
    case Join(false, a: Arr, false) => a.tresql
    case Join(false, e: Exp, false) => "[" + e.tresql + "]"
    case Join(true, null, false) => "/"
    case Join(true, e: Exp, false) => "/[" + e.tresql + "]"
    case _ => sys.error("Unexpected Join case")
  }
}

case class Obj(obj: Exp, alias: String, join: Join, outerJoin: String, nullable: Boolean = false)
  extends Exp {
  def tresql = {
    (if (join != null) join.tresql else "") + (if (outerJoin == "r") "?" else "") +
      obj.tresql + (if (outerJoin == "l") "?" else if (outerJoin == "i") "!" else "") +
      (obj match {
        case FunAsTable(_, cols, ord) =>
          " " + alias +
            cols
              .map(_.map(c => c.name + c.typ.map("::" + _).getOrElse(""))
                .mkString(if (ord) "(# " else "(", ", ", ")"))
              .getOrElse("")
        case _ => if (alias == null) "" else " " + alias
      })
  }
}
case class Col(col: Exp, alias: String) extends Exp {
  def tresql = any2tresql(col) + (if (alias != null) " " + alias else "")
}
case class Cols(distinct: Boolean, cols: List[Col]) extends Exp {
  def tresql = (if (distinct) "#" else "") + cols.map(_.tresql).mkString("{", ",", "}")
}
case class Grp(cols: List[Exp], having: Exp) extends Exp {
  def tresql = "(" + cols.map(any2tresql).mkString(",") + ")" +
    (if (having != null) "^(" + any2tresql(having) + ")" else "")
}
//cols expression is tuple in the form - ([<nulls first>], <order col list>, [<nulls last>])
case class Ord(cols: List[(Exp, Exp, Exp)]) extends Exp {
  def tresql = "#(" + cols.map(c => (if (c._1 == Null) "null " else "") +
    any2tresql(c._2) + (if (c._3 == Null) " null" else "")).mkString(",") + ")"
}
case class Query(tables: List[Obj], filter: Filters, cols: Cols,
                 group: Grp, order: Ord, offset: Exp, limit: Exp) extends Exp {
  def tresql = tables.map(any2tresql).mkString +
    filter.tresql +
    (if (cols != null) cols.tresql else "") +
    (if (group != null) any2tresql(group) else "") +
    (if (order != null) order.tresql else "") +
    (if (limit != null)
      s"@(${if (offset != null) any2tresql(offset) + " " else ""}${any2tresql(limit)})"
    else if (offset != null) s"@(${any2tresql(offset)}, )"
    else "")
}

case class WithTable(name: String, cols: List[String], recursive: Boolean, table: Exp)
  extends Exp {
  def tresql = s"$name (${if (!recursive) "# " else ""}${cols mkString ", "}) { ${table.tresql} }"
}
case class With(tables: List[WithTable], query: Exp) extends Exp {
  def tresql = tables.map(_.tresql).mkString(", ") + " " + query.tresql
}
case class Values(values: List[Arr]) extends Exp {
  def tresql = values map (_.tresql) mkString ", "
}
case class Insert(table: Ident, alias: String, cols: List[Col], vals: Exp, returning: Option[Cols])
  extends DMLExp {
  override def filter = null
  def tresql = "+" + table.tresql + Option(alias).map(" " + _).getOrElse("") +
    (if (cols.nonEmpty) cols.map(_.tresql).mkString("{", ",", "}") else "") +
    (if (vals != null) any2tresql(vals) else "") +
    returning.map(_.tresql).getOrElse("")
}
case class Update(table: Ident, alias: String, filter: Arr, cols: List[Col], vals: Exp, returning: Option[Cols])
  extends DMLExp {
  def tresql = {
    val filterTresql = if (filter != null) filter.tresql else ""
    val colsTresql = if (cols.nonEmpty) cols.map(_.tresql).mkString("{", ",", "}") else ""
    val valsTresql = if (vals != null) any2tresql(vals) else ""
    "=" + table.tresql + Option(alias).map(" " + _).getOrElse("") +
      (vals match {
        case vfs: ValuesFromSelect => valsTresql + filterTresql + colsTresql
        case _ => filterTresql + colsTresql + valsTresql
      }) +
      returning.map(_.tresql).getOrElse("")
  }
}
case class ValuesFromSelect(select: Query) extends Exp {
  def tresql =
    if (select.tables.size > 1) select.copy(tables = select.tables.tail).tresql
    else ""
}
case class Delete(table: Ident, alias: String, filter: Arr, using: Exp, returning: Option[Cols])
  extends DMLExp {
  override def cols = null
  override def vals = using
  def tresql = {
    val tbl =
      table.tresql + Option(alias).map(" " + _).getOrElse("") + (if (using == null) "" else using.tresql)

    "-" + tbl + filter.tresql + returning.map(_.tresql).getOrElse("")
  }
}
case class Arr(elements: List[Exp]) extends Exp {
  def tresql = "[" + any2tresql(elements) + "]"
}
case class Filters(filters: List[Arr]) extends Exp {
  def tresql = filters map any2tresql mkString
}
object All extends Exp {
  def tresql = "*"
}
case class IdentAll(ident: Ident) extends Exp {
  def tresql = ident.ident.mkString(".") + ".*"
}

trait Null extends Exp {
  def tresql = "null"
}
case object Null extends Null
case object NullUpdate extends Null

case class Braces(expr: Exp) extends Exp {
  def tresql = "(" + any2tresql(expr) + ")"
}

trait QueryParsers extends JavaTokenParsers with MemParsers with ExpTransformer {

  protected def macros: MacroResources = null

  def parseExp(exp: String): Exp = {
    phrase(exprList)(new scala.util.parsing.input.CharSequenceReader(exp)) match {
      case Success(r, _) => r
      case x => sys.error(x.toString)
    }
  }

  val reserved = Set("in", "null", "false", "true")

  //comparison operator regular expression
  val comp_op = """!?in\b|[<>=!~%$]+|`[^`]+`"""r

  val NoJoin = Join(default = false, null, noJoin = true)
  val DefaultJoin = Join(default = true, null, noJoin = false)

  //JavaTokenParsers overrides
  //besides standart whitespace symbols consider as a whitespace also comments in form /* comment */ and //comment
  override val whiteSpace = """([\h\v]*+(/\*(.|[\h\v])*?\*/)?(//.*+(\n|$))?)+"""r

  override def stringLiteral: MemParser[String] = ("""("[^"]*+")|('[^']*+')"""r) ^^ {
    case s => s.substring(1, s.length - 1)
  } named "string-literal"

  override def ident: MemParser[String] = super.ident ^? ({ case x if !(reserved contains x) => x },
      { x => s"'$x' is one of the reserved words: ${reserved.mkString(", ")}" }) named "ident"

  override def wholeNumber: MemParser[String] = ("""\d+"""r) named "number"

  def decimalNr: MemParser[BigDecimal] = decimalNumber ^^ (BigDecimal(_)) named "decimal-nr"

  /** Copied from RegexParsers to support comment handling as whitespace */
  override protected def handleWhiteSpace(source: java.lang.CharSequence, offset: Int): Int =
    if (skipWhitespace)
      whiteSpace findPrefixMatchOf new SubSequence(source, offset) match {
        case Some(matched) => offset + matched.end
        case None => offset
      }
    else
      offset
  //

  //literals
  def TRUE: MemParser[Boolean] = ("\\btrue\\b"r) ^^^ true named "true"
  def FALSE: MemParser[Boolean] = ("\\bfalse\\b"r) ^^^ false named "false"
  def NULL: MemParser[Null.type] = ("\\bnull\\b"r) ^^^ Null named "null"
  def ALL: MemParser[All.type] = "*" ^^^ All named "all"


  def const: MemParser[Const] = (TRUE | FALSE | decimalNr | stringLiteral) ^^ Const named "const"
  def sql: MemParser[Sql] = "`" ~> ("[^`]+"r) <~ "`" ^^ Sql named "sql"
  def qualifiedIdent: MemParser[Ident] = rep1sep(ident, ".") ^^ Ident named "qualified-ident"
  def qualifiedIdentAll: MemParser[IdentAll] = qualifiedIdent <~ ".*" ^^ IdentAll named "ident-all"
  def variable: MemParser[Variable] = ((":" ~> (
      rep1sep(ident | stringLiteral | wholeNumber, ".") ~ opt("?"))) | "?") ^^ {
    case "?" => Variable("?", null, opt = false)
    case ((i: String) :: (m: List[String @unchecked])) ~ o =>
      Variable(i, m, o != None)
  } named "variable"
  def id: MemParser[Id] = "#" ~> ident ^^ Id named "id"
  def idref: MemParser[IdRef] = ":#" ~> ident ^^ IdRef named "id-ref"
  def result: MemParser[Res] = (":" ~> wholeNumber <~ "(") ~ (wholeNumber | stringLiteral |
    qualifiedIdent) <~ ")" ^^ {
      case r ~ c => Res(r.toInt,
        c match {
          case s: String => try { s.toInt } catch { case _: NumberFormatException => s }
          case i: Ident => i
        })
    } named "result"
  def braces: MemParser[Braces] = "(" ~> expr <~ ")" ^^ Braces named "braces"
  /* Function parser must be applied before query because of the longest token
     * matching, otherwise qualifiedIdent of query will match earlier.
     * withQuery parser must be applied before function parser
     * array parser must be applied after query parser because it matches join parser
     * of the query.
     * insert, delete, update parsers must be applied before query parser
     * result parser must be applied before variable parser */
  def operand: MemParser[Exp] = (const | ALL | withQuery | function | insert | update | result |
    variable | query | id | idref  | array | sql) named "operand"
  /* function(<#> <arglist> <order by>). Maybe used in from clause so filter is not confused with join syntax.  */
  def functionWithoutFilter: MemParser[Fun] = (qualifiedIdent /* name */ <~ "(") ~
    opt("#") /* distinct */ ~ repsep(expr, ",") /* arglist */ ~
    ")" ~ opt(order) /* aggregate order */ ^^ {
    case Ident(n) ~ d ~ p ~ _ ~ o =>
      Fun(n.mkString("."), p, d.isDefined, o, None)
  }
  /* function(<#> <arglist> <order by>)<[filter]> */
  def function: MemParser[Exp] = (qualifiedIdent /* name */ <~ "(") ~
    opt("#") /* distinct */ ~ repsep(expr, ",") /* arglist */ ~
    ")" ~ opt(order) /* aggregate order */ ~ opt(filter) /* aggregate filter */ ^?({
    case Ident(n) ~ d ~ p ~ _ ~ o ~ f if f.map(_.elements.size).getOrElse(0) <= 1 =>
      Fun(n.mkString("."), p, d.isDefined, o, f.flatMap(_.elements.lift(0)))
  }, {
    case _ ~ _ ~ _ ~ _ ~ _ ~ f => s"Aggregate function filter must contain only one elements, instead of ${
      f.map(_.elements.size).getOrElse(0)}"
  }) ^^ { case f: Fun =>
    if (f.aggregateOrder.isEmpty && f.aggregateWhere.isEmpty &&
      macros != null && macros.isMacroDefined(f.name)) {
      macros.invokeMacro(f.name, QueryParsers.this, f.parameters)
    } else f
  } named "function"
  def array: MemParser[Arr] = "[" ~> repsep(expr, ",") <~ "]" ^^ Arr named "array"

  //query parsers
  def join: MemParser[Join] = (("/" ~ opt("[" ~> expr <~ "]")) | (opt("[" ~> expr <~ "]") ~ "/") |
    ";" | array) ^^ {
      case ";" => NoJoin
      case "/" ~ Some(e: Exp @unchecked) => Join(default = true, e, noJoin = false)
      case "/" ~ None => DefaultJoin
      case Some(e: Exp @unchecked) ~ "/" => Join(default = true, e, noJoin = false)
      case None ~ "/" => DefaultJoin
      case a: Exp => Join(default = false, a, noJoin = false)
    } named "join"
  def filter: MemParser[Arr] = array named "filter"
  def filters: MemParser[Filters] = rep(filter) ^^ Filters named "filters"
  private def objContent = const | functionWithoutFilter | variable | qualifiedIdent | braces
  private def alias =
    ident ~ opt("(" ~> opt("#") ~ rep1sep(ident ~ opt(cast), ",") <~ ")") ^^ {
      case id ~ Some(mbOrd ~ colDefs) =>
        (id, Some(colDefs.map { case cn ~ typ => TableColDef(cn, typ) }), mbOrd.isDefined)
      case id ~ None => (id, None, false)
    }
  def obj: MemParser[Obj] = opt(join) ~ opt("?") ~ objContent ~
    opt(opt("?" | "!") ~ alias ~ opt("?" | "!")) ^^ {
    case _ ~ Some(_) ~ _ ~ Some(Some(_) ~ _ ~ _ | _ ~ _ ~ Some(_)) =>
      sys.error("Cannot be right and left join at the same time")
    case join ~ rightoj ~ o ~ Some(leftoj ~ alias ~ leftoj1) =>
      def processAlias(coldefs: Option[List[TableColDef]], ord: Boolean) = {
        o match {
          case f: Fun => FunAsTable(f, coldefs, ord)
          case x if coldefs == None => x
          case x => sys.error(s"Table definition is allowed only after function. Instead found: ${x.tresql}")
        }
      }
      Obj(processAlias(alias._2, alias._3), alias._1, join.orNull,
        rightoj.map(x => "r") orElse (leftoj orElse leftoj1).map(j => if(j == "?") "l" else "i") orNull,
        (leftoj orElse leftoj1).exists(_ == "?"))
    case join ~ rightoj ~ o ~ None => Obj(o, null, join orNull, rightoj.map(x => "r") orNull)
  } named "obj"
  def objWithJoin: MemParser[Obj] = obj ^? ({
    case obj if obj.join != null => obj
  } , {
    case obj => s"no join condition found on object: ${obj.tresql}"
  }) named "obj-with-join"
  def objs: MemParser[List[Obj]] = obj ~ rep(objWithJoin ~ opt("?" | "!")) ^^ {
    case o ~ l =>
      var prev: Obj = null
      val res = (o :: l.map {
        case o ~ Some(oj) if "r" == o.outerJoin => sys.error("Cannot be right and left join at the same time")
        case o ~ Some(oj) => o.copy(outerJoin = if (oj == "?") "l" else "i", nullable = oj == "?")
        case o ~ None => o
      }).flatMap { thisObj =>
        val (prevObj, prevAlias) = (prev, if (prev == null) null else prev.alias)
        prev = thisObj
        //process foreign key shortcut join
        thisObj match {
          case o @ Obj(_, a, j @ Join(false, Arr(l @ List(o1 @ Obj(_, a1, _, oj1, n1), _*)), false), oj, n) =>
            //o1.alias prevail over o.alias, o.{outerJoin, nullable} prevail over o1.{outerJoin, nullable}
            List(o.copy(alias = Option(a1).getOrElse(a), join =
              j.copy(expr = o1.copy(alias = null, outerJoin = null, nullable = false)),
              outerJoin = if (oj == null) oj1 else oj, nullable = n || n1)) ++
              (if (prevObj == null) List() else l.tail.flatMap {
                //flattenize array of foreign key shortcut joins
                case o2 @ Obj(_, a2, _, oj2, n2) => List(if (prevAlias != null) Obj(Ident(List(prevAlias)),
                  null, NoJoin, null)
                else Obj(prevObj.obj, null, NoJoin, null),
                  o.copy(alias = if (a2 != null) a2 else o.alias, join =
                    j.copy(expr = o2.copy(alias = null, outerJoin = null, nullable = false)),
                    outerJoin = if (oj == null) oj2 else oj, nullable = n || n2))
                case x => List(o.copy(join = Join(default = false, x, noJoin = false)))
              })
          case o => List(o)
        }
      }
      Vector(res: _*).lastIndexWhere(_.outerJoin == "r") match {
        case -1 => res
        //set nullable flag for all objs right to the last obj with outer join
        case x => res.zipWithIndex.map(t => if (t._2 < x && !t._1.nullable) t._1.copy(nullable = true) else t._1)
      }
  } named "objs"
  def column: MemParser[Col] = (qualifiedIdentAll | (expr ~ opt(stringLiteral | qualifiedIdent))) ^^ {
    case i: IdentAll => Col(i, null)
    //move object alias to column alias
    case (o @ Obj(_, a, _, _, _)) ~ (Some(ca)) if a != null =>
      sys.error(s"Column cannot have two aliases: `$a`, `$ca`")
    case (o @ Obj(_, a, _, _, _))  ~ None => Col(o.copy(alias = null), a)
    case (e: Exp @unchecked) ~ (a: Option[_]) => Col(e, a map {
      case Ident(i) => i.mkString; case s => "\"" + s + "\""
    } orNull)
  } ^^ { pr =>
    def extractAlias(expr: Exp): (String, Exp) = expr match {
      case t: TerOp => extractAlias(t.content)
      case o@BinOp(_, lop, rop) =>
        val x = extractAlias(rop)
        (x._1, o.copy(rop = x._2))
      case o@UnOp(_, op) =>
        val x = extractAlias(op)
        (x._1, o.copy(operand = x._2))
      case o@Obj(_, alias, _, null, _) if alias != null => (alias, o.copy(alias = null))
      case o => (null, o)
    }
    pr match {
      case c @ Col(_: IdentAll | _: Obj, _) => c
      case c @ Col(e, null) => extractAlias(e) match {
        case (null, _) => c
        case (a, e) =>
          c.copy(col = e, alias = a)
      }
      case r => r
    }
  } named "column"
  def columns: MemParser[Cols] = (opt("#") <~ "{") ~ rep1sep(column, ",") <~ "}" ^^ {
    case d ~ c => Cols(d.isDefined, c)
  } named "columns"
  def group: MemParser[Grp] = ("(" ~> rep1sep(expr, ",") <~ ")") ~
    opt(("^" ~ "(") ~> expr <~ ")") ^^ { case g ~ h => Grp(g, if (h.isEmpty) null else h.get) }  named "group"
  def orderMember: MemParser[(Exp, Exp, Exp)] = opt(NULL) ~ expr ~ opt(NULL) ^^ {
    case Some(nf) ~ e ~ Some(nl) => sys.error("Cannot be nulls first and nulls last at the same time")
    case nf ~ e ~ nl => (if (nf.isEmpty) null else nf.get, e, if (nl.isEmpty) null else nl.get)
  } named "order-member"
  def order: MemParser[Ord] = ("#" ~ "(") ~> rep1sep(orderMember, ",") <~ ")" ^^ Ord named "order"
  def offsetLimit: MemParser[(Exp, Exp)] = ("@" ~ "(") ~> (wholeNumber | variable) ~ opt(",") ~
    opt(wholeNumber | variable) <~ ")" ^^ { pr =>
      {
        def c(x: Any) = x match { case v: Variable => v case s: String => Const(BigDecimal(s)) }
        pr match {
          case o ~ comma ~ Some(l) => (c(o), c(l))
          case o ~ Some(comma) ~ None => (c(o), null)
          case o ~ None ~ None => (null, c(o))
        }
      }
    } named "offset-limit"
  def query: MemParser[Exp] = ((objs | NULL) ~ filters ~ opt(columns ~ opt(group)) ~ opt(order) ~
    opt(offsetLimit) | (columns ~ filters)) ^^ {
      case Null ~ Filters(Nil) ~ None ~ None ~ None => Null //null literal
      case List(t) ~ Filters(Nil) ~ None ~ None ~ None => t match {
        case Obj(b: Braces, null, j, null, _) =>
          //transform potential join to parent into query
          Option(j).map(transformHeadJoin(_)(b)).getOrElse(b)
        case x => x
      }
      case t ~ (f: Filters) ~ cgo ~ (o: Option[Ord @unchecked]) ~ (l: Option[(Exp, Exp) @unchecked]) =>
        val cg: (Cols, Grp) = cgo match {
          case Some((c: Cols) ~ Some(g: Grp)) => (c, g)
          case Some((c: Cols) ~ None) => (c, null)
          case None => (null, null)
        }
        Query(t match {
          case tables: List[Obj @unchecked] => tables
          case Null => List(Obj(Null, null, null, null))
        }, f, cg._1, cg._2, o.orNull, l.map(_._1) orNull, l.map(_._2) orNull)
      case (c: Cols) ~ (f: Filters) => Query(List(Obj(Null, null, null, null)), f, c,
        null, null, null, null)
    } ^^ { pr =>
      def toDivChain(objs: List[Obj]): Exp = objs match {
        case o :: Nil => o.obj
        case l => BinOp("/", l.head.obj, toDivChain(l.tail))
      }
      pr match {
        case Query(objs, Filters(Nil), null, null, null, null, null) if objs forall {
          case Obj(_, null, DefaultJoin, null, _) | Obj(_, null, null, null, _) => true
          case _ => false
        } => toDivChain(objs)
        case q: Exp => q
      }
    } named "query"

  def queryWithCols: MemParser[Exp] = query ^? ({
      case q @ Query(objs, _, cols, _, _, _, _) if cols != null => q
    }, {case x => "Query must contain column clause: " + x}) named "query-with-cols"

  def withTable: MemParser[WithTable] =
    ((ident <~ "(") ~ opt("#") ~ (ALL | repsep(ident, ",")) <~ (")" ~ "{")) ~ (expr <~ "}") ^^ {
      case name ~ distinct ~ (cols: List[String]) ~ exp => WithTable(name, cols, distinct.isEmpty, exp)
      case name ~ distinct ~ All ~ exp => WithTable(name, Nil, distinct.isEmpty, exp)
    } named "with-table"
  def withQuery: MemParser[With] = opt(join) ~ rep1sep(withTable, ",") ~ opt(expr) ^? ({
    case optJoin ~ (wts @ List(wt)) ~ None =>
      val q = Obj(Ident(List(wt.name)), null, null, null, false) // select * from <wt.name>
      With(wts, optJoin.map(transformHeadJoin(_)(q)).getOrElse(q))
    case optJoin ~ wts ~ Some(q) => With(wts, optJoin.map(transformHeadJoin(_)(q)).getOrElse(q))
  }, {
    case _ ~ wts ~ _ => s"If query is omitted only one with cursor definition is allowed, instead found: ${
      wts.map(_.tresql).mkString(",")}"
  }) named "with-query"

  def values: MemParser[Values] = rep1sep(array, ",") ^^ Values named "values"
  def valuesSelect: MemParser[Exp] = (withQuery | queryWithCols) ~ rep(
    ("++" | "+" | "-" | "&&") ~ (withQuery | queryWithCols)) ^^ binOp named "values-select"

  def insert: MemParser[Insert] =
    (("+" ~> qualifiedIdent ~ opt(ident) ~ opt(columns) ~ opt(valuesSelect | repsep(array, ","))) |
    ((qualifiedIdent ~ opt(ident) ~ opt(columns) <~ "+") ~ values)) ~ opt(columns) ^^ {
      case t ~ a ~ c ~ (v: Option[_]) ~ maybeCols =>
        Insert(
          t, a.orNull, c.map(_.cols).getOrElse(Nil),
          v.map { case v: List[Arr] @unchecked => Values(v) case s: Exp @unchecked => s } orNull,
          maybeCols
        )
      case t ~ a ~ c ~ (v: Values @unchecked) ~ maybeCols =>
        Insert(t, a.orNull, c.map(_.cols).getOrElse(Nil), v, maybeCols)
      case x => sys.error(s"Unexpected insert parse result: $x")
    } named "insert"

  //UPDATE parsers
  // update table set col1 = val1, col2 = val2 where ...
  private def simpleUpdate: MemParser[Update] =
    (("=" ~> qualifiedIdent ~ opt(ident) ~ opt(filter) ~ opt(columns) ~ opt(array)) |
      ((qualifiedIdent ~ opt(ident) ~ opt(filter) ~ opt(columns) <~ "=") ~ array)) ~ opt(columns) ^^ {
        case (t: Ident) ~ (a: Option[String] @unchecked) ~ f ~ c ~ v ~ maybeCols =>
          Update(
            t, a orNull, f orNull, c.map(_.cols).getOrElse(Nil), v match {
              case a: Arr => a
              case Some(vals: Exp @unchecked) => vals
              case None => null
            },
            maybeCols
          )
     } named "simple-update"
  //update table set (col1, col2) = (select col1, col2 from table 2 ...) where ...
  private def updateColsSelect: MemParser[Update] =
    "=" ~> qualifiedIdent ~ opt(ident) ~ opt(filter) ~ columns ~ valuesSelect ~ opt(columns) ^^
      {
        case (t: Ident) ~ (a: Option[String] @unchecked) ~ f ~ c ~ v ~ maybeCols =>
          Update(t, a orNull, f orNull, c.cols, v, maybeCols)
      } ^?
      ({
        case u if u.cols.nonEmpty && u.cols.forall {
          case Col(BinOp("=", _, _), _) => false
          case _ => true
        } => u
      }, {
        case x => s"Columns must not contain assignement operation. Instead encountered: ${x.tresql}"
      }) named "update-cols-select"
  //update table set col1 = sel_col1, col2 = sel_col2 from (select sel_col1, sel_col2 from table2 ...) values_table where ....
  private def updateFromSelect: MemParser[Update] = "=" ~> objs ~ opt(filter) ~ columns ~ opt(columns) ^? ({
    case (tables @ Obj(updateTable @ Ident(_), alias, _, _, _) :: _) ~ f ~ c ~ maybeCols =>
      Update(
        updateTable,
        alias,
        f.orNull,
        c.cols,
        ValuesFromSelect(
          Query(tables = tables, Filters(Nil), null, null, null, null, null)
        ),
        maybeCols
      )
  }, {
    case (objs: List[Obj] @unchecked) ~ _ ~ _ ~ _ => "Update tables clause must as the first element have " +
     "qualifiedIdent with optional alias as a table to be updated" +
     s"Instead encountered: ${objs.map(_.tresql).mkString}"
  }) ^? ({
    case u if u.cols.nonEmpty && u.cols.forall {
      case Col(BinOp("=", Obj(_: Ident, _, _, _, _), _), _) => true
      case _ => false
    } => u.copy(cols = u.cols.map {
      //set assignable value to NullUpdate so null update expression is col = null, not col is null
      case c @ Col(a @ BinOp(_, _, Null), _) => c.copy(col = a.copy(rop = NullUpdate))
      case x => x
    })
  }, {
    case _ => "Update from select columns must be assignment expressions"
  }) named "update-from-select"
  def update: MemParser[Update] = (updateColsSelect | updateFromSelect | simpleUpdate) named "update"
  //END UPDATE parsers

  def delete: MemParser[Delete] = (("-" ~> objs ~ filter) |
    ((objs <~ "-") ~ filter)) ~ opt(columns) ^? ({
      case (tables @ Obj(delTable @ Ident(_), alias, _, _, _) :: using) ~ f ~ maybeCols =>
        val vfs =
          if (using.nonEmpty)
            ValuesFromSelect(
              Query(
                tables = tables,
                Filters(Nil),
                null, null, null, null, null
              )
            )
          else null
        Delete(delTable, alias, f, vfs, maybeCols)
    }, {
      case (objs: List[Obj] @unchecked) ~ _ ~ _ => "Delete tables clause must as the first element have " +
        "qualifiedIdent with optional alias as a table to be deleted from" +
        s"Instead encountered: ${objs.map(_.tresql).mkString}"
    }) named "delete"
  //operation parsers
  //delete must be before alternative since it can start with - sign and
  //so it is not translated into minus expression!
  def unaryExpr: MemParser[Exp] = delete | (opt("-" | "!" | "|" | "~") ~ operand) ^^ {
      case a ~ (b: Exp) => a.map(UnOp(_, b)).getOrElse(b)
      case x: Exp => x
    } named "unary-exp"
  private def cast = "::" ~> (ident | stringLiteral)
  def castExpr: MemParser[Exp] = unaryExpr ~ opt(cast) ^^ {
    case e ~ Some(t) => Cast(e, t)
    case e ~ None => e
  } named "cast-expr" //postgresql style type conversion operator
  def mulDiv: MemParser[Exp] = castExpr ~ rep("*" ~ castExpr | "/" ~ castExpr) ^^ binOp named "mul-div"
  def plusMinus: MemParser[Exp] = mulDiv ~ rep(("++" | "+" | "-" | "&&" | "||") ~ mulDiv) ^^ binOp named "plus-minus"
  def comp: MemParser[Exp] = plusMinus ~ rep(comp_op ~ plusMinus) ^? (
      {
        case lop ~ Nil => lop
        case lop ~ ((o ~ rop) :: Nil) => BinOp(o, lop, rop)
        case lop ~ List((o1 ~ mop), (o2 ~ rop)) => TerOp(lop, o1, mop, o2, rop)
      },
      {
        case lop ~ x => "Ternary comparison operation is allowed, however, here " + (x.size + 1) +
          " operands encountered."
      }) named "comp"
  def in: MemParser[In] = plusMinus ~ ("in" | "!in") ~ "(" ~ rep1sep(plusMinus, ",") <~ ")" ^^ {
    case lop ~ "in" ~ "(" ~ rop => In(lop, rop, not = false)
    case lop ~ "!in" ~ "(" ~ rop => In(lop, rop, not = true)
  } named "in"
  //in parser should come before comp so that it is not taken for in function which is illegal
  def logicalOp: MemParser[Exp] = in | comp named "logical-op"
  def expr: MemParser[Exp] = logicalOp ~ rep("&" ~ logicalOp | "|" ~ logicalOp) ^^ binOp named "expr"
  def exprList: MemParser[Exp] = repsep(expr, ",") ^^ {
    case e :: Nil => e
    case l => Arr(l)
  } named "expr-list"

  private def binOp(p: Exp ~ List[String ~ Exp]): Exp = p match {
    case lop ~ Nil => lop
    case lop ~ ((o ~ rop) :: l) => BinOp(o, lop, binOp(QueryParsers.this.~(rop, l)))
  }

  private def transformHeadJoin(join: Join): Transformer = transformer {
    case Braces(e) => Braces(transformHeadJoin(join)(e))
    case With(wt, q) => With(wt, transformHeadJoin(join)(q))
    case q: Query =>
      q.copy(tables = q.tables.updated(0, transformHeadJoin(join)(q.tables.head).asInstanceOf[Obj]))
    case o: Obj => o.copy(join = join) //set join to parent
  }

  /** Copied from scala.util.parsing.combinator.SubSequence since it cannot be instantiated. */
  // A shallow wrapper over another CharSequence (usually a String)
  //
  // See SI-7710: in jdk7u6 String.subSequence stopped sharing the char array of the original
  // string and began copying it.
  // RegexParsers calls subSequence twice per input character: that's a lot of array copying!
  private class SubSequence(s: CharSequence, start: Int, val length: Int) extends CharSequence {
    def this(s: CharSequence, start: Int) = this(s, start, s.length - start)

    def charAt(i: Int) =
      if (i >= 0 && i < length) s.charAt(start + i) else throw new IndexOutOfBoundsException(s"index: $i, length: $length")

    def subSequence(_start: Int, _end: Int) = {
      if (_start < 0 || _end < 0 || _end > length || _start > _end)
        throw new IndexOutOfBoundsException(s"start: ${_start}, end: ${_end}, length: $length")

      new SubSequence(s, start + _start, _end - _start)
    }

    override def toString = s.subSequence(start, start + length).toString
  }
}
