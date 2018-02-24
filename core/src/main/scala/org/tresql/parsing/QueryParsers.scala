package org.tresql.parsing

import scala.util.parsing.combinator.JavaTokenParsers

trait QueryParsers extends JavaTokenParsers with MemParsers {

  val reserved = Set("in", "null", "false", "true")

  //comparison operator regular expression
  val comp_op = """!?in\b|[<>=!~%$]+"""r

  private val simple_ident_regex = "^[_\\p{IsLatin}][_\\p{IsLatin}0-9]*$".r

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

  trait Exp {
    def tresql: String
  }

  trait DMLExp extends Exp {
    def table: Ident
    def alias: String
    def cols: List[Col]
    def filter: Arr
    def vals: Exp
  }

  case class Const(value: Any) extends Exp {
    def tresql = any2tresql(value)
  }
  case class Ident(ident: List[String]) extends Exp {
    def tresql = ident.mkString(".")
  }
  case class Variable(variable: String, members: List[String], opt: Boolean) extends Exp {
    def tresql = if (variable == "?") "?" else {
      ":" +
        (if (simple_ident_regex.pattern.matcher(variable).matches) variable
         else if (variable contains "'") "\"" + variable + "\""
         else "'" + variable + "'") +
        (if (members == null | members == Nil) "" else "." + (members map any2tresql mkString ".")) +
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
      (if (simple_ident_regex.pattern.matcher(typ).matches) typ else "'" + typ + "'")
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
  val NoJoin = Join(default = false, null, noJoin = true)
  val DefaultJoin = Join(default = true, null, noJoin = false)

  case class Obj(obj: Exp, alias: String, join: Join, outerJoin: String, nullable: Boolean = false)
      extends Exp {
    def tresql = (if (join != null) join.tresql else "") + (if (outerJoin == "r") "?" else "") +
      obj.tresql + (if (outerJoin == "l") "?" else if (outerJoin == "i") "!" else "") +
      (if (alias != null) " " + alias else "")
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
  case class Insert(table: Ident, alias: String, cols: List[Col], vals: Exp) extends DMLExp {
    override def filter = null
    def tresql = "+" + table.tresql + Option(alias).map(" " + _).getOrElse("") +
      (if (cols.nonEmpty) cols.map(_.tresql).mkString("{", ",", "}") else "") +
      (if (vals != null) any2tresql(vals) else "")
  }
  case class Update(table: Ident, alias: String, filter: Arr, cols: List[Col], vals: Exp) extends DMLExp {
    def tresql = "=" + table.tresql + Option(alias).map(" " + _).getOrElse("") +
      (if (filter != null) filter.tresql else "") +
      (if (cols.nonEmpty) cols.map(_.tresql).mkString("{", ",", "}") else "") +
      (if (vals != null) any2tresql(vals) else "")
  }
  case class ValuesFromSelect(select: Exp) extends Exp {
    def tresql = select.tresql
  }
  case class Delete(table: Ident, alias: String, filter: Arr) extends DMLExp {
    override def cols = null
    override def vals = null
    def tresql = "-" + table.tresql + Option(alias).map(" " + _).getOrElse("") + filter.tresql
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
  object Null extends Exp {
    def tresql = "null"
  }

  case class Braces(expr: Exp) extends Exp {
    def tresql = "(" + any2tresql(expr) + ")"
  }

  def any2tresql(any: Any): String = any match {
    case a: String => if (a.contains("'")) "\"" + a + "\"" else "'" + a + "'"
    case e: Exp @unchecked => e.tresql
    case null => "null"
    case l: List[_] => l map any2tresql mkString ", "
    case x => x.toString
  }

  //literals
  def TRUE: MemParser[Boolean] = ("\\btrue\\b"r) ^^^ true named "true"
  def FALSE: MemParser[Boolean] = ("\\bfalse\\b"r) ^^^ false named "false"
  def NULL: MemParser[Null.type] = ("\\bnull\\b"r) ^^^ Null named "null"
  def ALL: MemParser[All.type] = "*" ^^^ All named "all"


  def const: MemParser[Const] = (TRUE | FALSE | decimalNr | stringLiteral) ^^ Const named "const"
  def qualifiedIdent: MemParser[Ident] = rep1sep(ident, ".") ^^ Ident named "qualified-ident"
  def qualifiedIdentAll: MemParser[IdentAll] = qualifiedIdent <~ ".*" ^^ IdentAll named "ident-all"
  def variable: MemParser[Variable] = ((":" ~> ((ident | stringLiteral) ~
      rep("." ~> (ident | stringLiteral | wholeNumber)) ~ opt(":" ~> ident) ~ opt("?"))) | "?") ^^ {
    case "?" => Variable("?", null, opt = false)
    case (i: String) ~ (m: List[String @unchecked]) ~ (t: Option[String @unchecked]) ~ o => Variable(i, m, o != None)
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
     * insert, delete, update parsers must be applied before query parser */
  def operand: MemParser[Exp] = (const | ALL | withQuery | function | insert | update | variable |
    query | id | idref | result | array) named "operand"
  /* function(<#> <arglist> <order by>)<[filter]> */
  def function: MemParser[Fun] = (qualifiedIdent /* name */ <~ "(") ~
    opt("#") /* distinct */ ~ repsep(expr, ",") /* arglist */ ~
    ")" ~ opt(order) /* aggregate order */ ~ opt(filter) /* aggregate filter */ ^?({
    case Ident(n) ~ d ~ p ~ _ ~ o ~ f if f.map(_.elements.size).getOrElse(0) <= 1 =>
      Fun(n.mkString("."), p, d.isDefined, o, f.flatMap(_.elements.lift(0)))
  }, {
    case _ ~ _ ~ _ ~ _ ~ _ ~ f => s"Aggregate function filter must contain only one elements, instead of ${
      f.map(_.elements.size).getOrElse(0)}"
  }) named "function"
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
  private def objContent = const | function | variable | qualifiedIdent | braces
  def obj: MemParser[Obj] = opt(join) ~ opt("?") ~ objContent ~
    opt(opt("?" | "!") ~ ident ~ opt("?" | "!")) ^^ {
    case _ ~ Some(_) ~ _ ~ Some(Some(_) ~ _ ~ _ | _ ~ _ ~ Some(_)) =>
      sys.error("Cannot be right and left join at the same time")
    case join ~ rightoj ~ o ~ Some(leftoj ~ alias ~ leftoj1) =>
      Obj(o, alias, join.orNull,
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
      case List(t) ~ Filters(Nil) ~ None ~ None ~ None => t
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
        case q: Exp @unchecked => q
      }
    } named "query"

  def queryWithCols: MemParser[Exp] = query ^? ({
      case q @ Query(objs, _, cols, _, _, _, _) if cols != null => q
    }, {case x => "Query must contain column clause: " + x}) named "query-with-cols"

  def withTable: MemParser[WithTable] = (function <~ "{") ~ (expr <~ "}") ^? ({
    case f ~ (q: Exp @unchecked) if f.parameters.forall {
      case Obj(Ident(List(_)), _, _, _, _) => true
      case x => false
    } || f.parameters == List(All) =>
      WithTable(
        f.name,
        f.parameters.flatMap { case Obj(Ident(l @ List(_)), _, _, _, _) => l case _ => Nil },
        !f.distinct,
        q
      )
  }, {
    case x ~ q => s"with table definition must contain simple column names as strings, instead - ${x.tresql}"
  }) named "with-table"
  def withQuery: MemParser[With] = rep1sep(withTable, ",") ~ expr ^? ({
    case wts ~ (q: Exp @unchecked) => With(wts, q)
  }, {
    case wts ~ q => s"Unsupported with query: $q"
  }) named "with-query"

  def values: MemParser[Values] = rep1sep(array, ",") ^^ Values named "values"
  def valuesSelect: MemParser[Exp] = queryWithCols ~ rep(
    ("++" | "+" | "-" | "&&") ~ queryWithCols) ^^ binOp named "values-select"

  def insert: MemParser[Insert] = (("+" ~> qualifiedIdent ~ opt(ident) ~ opt(columns) ~
    opt(valuesSelect | repsep(array, ","))) |
    ((qualifiedIdent ~ opt(ident) ~ opt(columns) <~ "+") ~ values)) ^^ {
      case t ~ a ~ c ~ (v: Option[_]) =>
        Insert(t, a.orNull, c.map(_.cols).getOrElse(Nil),
          v.map { case v: List[Arr] @unchecked => Values(v) case s: Exp @unchecked => s } orNull)
      case t ~ a ~ c ~ (v: Values @unchecked) =>
        Insert(t, a.orNull, c.map(_.cols).getOrElse(Nil), v)
      case x => sys.error(s"Unexpected insert parse result: $x")
    } named "insert"

  //UPDATE parsers
  // update table set col1 = val1, col2 = val2 where ...
  private def simpleUpdate: MemParser[Update] =
    (("=" ~> qualifiedIdent ~ opt(ident) ~ opt(filter) ~ opt(columns) ~ opt(array)) |
      ((qualifiedIdent ~ opt(ident) ~ opt(filter) ~ opt(columns) <~ "=") ~ array)) ^^ {
        case (t: Ident) ~ (a: Option[String] @unchecked) ~ f ~ c ~ v => Update(
          t, a orNull, f orNull, c.map(_.cols).getOrElse(Nil), v match {
            case a: Arr => a
            case Some(vals: Exp @unchecked) => vals
            case None => null
          })
      } named "simple-update"
  //update table set (col1, col2) = (select col1, col2 from table 2 ...) where ...
  private def updateColsSelect: MemParser[Update] =
    "=" ~> qualifiedIdent ~ opt(ident) ~ opt(filter) ~ columns ~ valuesSelect ^^ {
        case (t: Ident) ~ (a: Option[String] @unchecked) ~ f ~ c ~ v =>
          Update(t, a orNull, f orNull, c.cols, v)
      } named "update-cols-select"
  //update table set col1 = sel_col1, col2 = sel_col2 from (select sel_col1, sel_col2 from table2 ...) values_table where ....
  private def updateFromSelect: MemParser[Update] = "=" ~> objs ~ opt(filter) ~ opt(columns) ^? ({
    case (tables @ Obj(updateTable @ Ident(_), alias, _, _, _) :: fromTables) ~ f ~ c if fromTables.nonEmpty =>
      Update(updateTable, alias, f.orNull, c.map(_.cols).getOrElse(Nil),
        ValuesFromSelect(Query(tables = tables, Filters(Nil), Cols(false, List(Col(All, null))), null, null, null, null)))
   }, {
   case (objs: List[Obj] @unchecked) ~ _ ~ _ => "Update tables clause must as the first element must have - " +
     "qualifiedIdent with optional alias as a table to be updated" +
     s"Instead encountered: ${objs.map(_.tresql).mkString}"
  }) named "update-from-select"
  def update: MemParser[Update] = (updateColsSelect | updateFromSelect | simpleUpdate) named "update"
  //END UPDATE parsers

  def delete: MemParser[Delete] = (("-" ~> qualifiedIdent ~ opt(ident) ~ filter) |
    (((qualifiedIdent ~ opt(ident)) <~ "-") ~ filter)) ^^ {
      case t ~ a ~ f => Delete(t, a orNull, f)
    } named "delete"
  //operation parsers
  //delete must be before alternative since it can start with - sign and
  //so it is not translated into minus expression!
  def unaryExpr: MemParser[Exp] = delete | (opt("-" | "!" | "|" | "~") ~ operand) ^^ {
      case a ~ (b: Exp) => a.map(UnOp(_, b)).getOrElse(b)
      case x: Exp => x
    } named "unary-exp"
  def cast: MemParser[Exp] = unaryExpr ~ opt("::" ~> (ident | stringLiteral)) ^^ {
    case e ~ Some(t) => Cast(e, t)
    case e ~ None => e
  } named "cast" //postgresql style type conversion operator
  def mulDiv: MemParser[Exp] = cast ~ rep("*" ~ cast | "/" ~ cast) ^^ binOp named "mul-div"
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
    case lop ~ ((o ~ rop) :: l) => BinOp(o, lop, binOp(this.~(rop, l)))
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
