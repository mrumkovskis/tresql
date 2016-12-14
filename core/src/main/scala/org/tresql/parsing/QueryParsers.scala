package org.tresql.parsing

import scala.util.parsing.combinator.JavaTokenParsers

trait QueryParsers extends JavaTokenParsers with MemParsers {

  val reserved = Set("in", "null", "false", "true")

  //comparison operator regular expression
  val comp_op = """!?in\b|[<>=!~%$]+"""r

  //JavaTokenParsers overrides
  //besides standart whitespace symbols consider as a whitespace also comments in form /* comment */ and //comment
  override val whiteSpace = """(\s*+(/\*(.|\s)*?\*/)?(//.*+(\n|$))?)+"""r

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
      (whiteSpace findPrefixMatchOf (new SubSequence(source, offset))) match {
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
    def vals: Any
  }

  case class Ident(ident: List[String]) extends Exp {
    def tresql = ident.mkString(".")
  }
  case class Variable(variable: String, members: List[String], typ: String, opt: Boolean) extends Exp {
    def tresql = if (variable == "?") "?" else {
      ":" +
        (if (variable contains "'") "\"" + variable + "\"" else "'" + variable + "'") +
        (if (members == null | members == Nil) "" else "." + (members map any2tresql mkString ".")) +
        (if (typ == null) "" else ": " + typ) +
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
  case class UnOp(operation: String, operand: Any) extends Exp {
    def tresql = operation + any2tresql(operand)
  }
  case class Fun(name: String, parameters: List[Any], distinct: Boolean) extends Exp {
    def tresql = name + "(" + (if (distinct) "# " else "") +
      ((parameters map any2tresql) mkString ", ") + ")"
  }
  case class In(lop: Any, rop: List[Any], not: Boolean) extends Exp {
    def tresql = any2tresql(lop) + (if (not) " !" else " ") + rop.map(any2tresql).mkString(
      "in(", ", ", ")")
  }
  case class BinOp(op: String, lop: Any, rop: Any) extends Exp {
    def tresql = any2tresql(lop) + " " + op + " " + any2tresql(rop)
  }
  case class TerOp(lop: Any, op1: String, mop: Any, op2: String, rop: Any) extends Exp {
    def content = BinOp("&", BinOp(op1, lop, mop), BinOp(op2, mop, rop))
    def tresql = s"${any2tresql(lop)} $op1 ${any2tresql(mop)} $op2 ${any2tresql(rop)}"
  }

  case class Join(default: Boolean, expr: Any, noJoin: Boolean) extends Exp {
    def tresql = this match {
      case Join(_, _, true) => ";"
      case Join(false, a: Arr, false) => a.tresql
      case Join(false, e: Exp @unchecked, false) => "[" + e.tresql + "]"
      case Join(true, null, false) => "/"
      case Join(true, e, false) => "/[" + any2tresql(e) + "]"
      case _ => sys.error("Unexpected Join case")
    }
  }
  val NoJoin = Join(false, null, true)
  val DefaultJoin = Join(true, null, false)

  case class Obj(obj: Exp, alias: String, join: Join, outerJoin: String, nullable: Boolean = false)
      extends Exp {
    def tresql = (if (join != null) join.tresql else "") + (if (outerJoin == "r") "?" else "") +
      obj.tresql + (if (outerJoin == "l") "?" else if (outerJoin == "i") "!" else "") +
      (if (alias != null) " " + alias else "")
  }
  case class Col(col: Any, alias: String, typ: String) extends Exp {
    def tresql = any2tresql(col) + (if (typ != null) " :" + typ else "") +
      (if (alias != null) " " + alias else "")
  }
  case class Cols(distinct: Boolean, cols: List[Col]) extends Exp {
    def tresql = (if (distinct) "#" else "") + cols.map(_.tresql).mkString("{", ",", "}")
  }
  case class Grp(cols: List[Any], having: Any) extends Exp {
    def tresql = "(" + cols.map(any2tresql).mkString(",") + ")" +
      (if (having != null) "^(" + any2tresql(having) + ")" else "")
  }
  //cols expression is tuple in the form - ([<nulls first>], <order col list>, [<nulls last>])
  case class Ord(cols: List[(Exp, Any, Exp)]) extends Exp {
    def tresql = "#(" + cols.map(c => (if (c._1 == Null) "null " else "") +
      any2tresql(c._2) + (if (c._3 == Null) " null" else "")).mkString(",") + ")"
  }
  case class Query(tables: List[Obj], filter: Filters, cols: Cols,
    group: Grp, order: Ord, offset: Any, limit: Any) extends Exp {
    def tresql = tables.map(any2tresql).mkString +
      filter.tresql +
      (if (cols != null) cols.tresql else "") +
      (if (group != null) any2tresql(group) else "") +
      (if (order != null) order.tresql else "") +
      (if (limit != null)
        s"@(${(if (offset != null) any2tresql(offset) + " " else "")}${any2tresql(limit)})"
      else if (offset != null) s"@(${any2tresql(offset)}, )"
      else "")
  }

  case class WithTable(name: String, cols: List[String], recursive: Boolean, table: Exp)
    extends Exp {
    def tresql = s"$name (${cols mkString ", "}) { ${table.tresql} }"
  }
  case class With(tables: List[WithTable], query: Exp) extends Exp {
    def tresql = tables.map(_.tresql).mkString(", ") + " " + query.tresql
  }

  case class Insert(table: Ident, alias: String, cols: List[Col], vals: Any) extends DMLExp {
    override def filter = null
    def tresql = "+" + table.tresql + Option(alias).map(" " + _).getOrElse("") +
      (if (cols != null) cols.map(_.tresql).mkString("{", ",", "}") else "") +
      (if (vals != null) any2tresql(vals) else "")
  }
  case class Update(table: Ident, alias: String, filter: Arr, cols: List[Col], vals: Any) extends DMLExp {
    def tresql = "=" + table.tresql + Option(alias).map(" " + _).getOrElse("") +
      (if (filter != null) filter.tresql else "") +
      (if (cols != null) cols.map(_.tresql).mkString("{", ",", "}") else "") +
      (if (vals != null) any2tresql(vals) else "")
  }
  case class Delete(table: Ident, alias: String, filter: Arr) extends DMLExp {
    override def cols = null
    override def vals = null
    def tresql = "-" + table.tresql + Option(alias).map(" " + _).getOrElse("") + filter.tresql
  }
  case class Arr(elements: List[Any]) extends Exp {
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

  case class Braces(expr: Any) extends Exp {
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

  def qualifiedIdent: MemParser[Ident] = rep1sep(ident, ".") ^^ Ident named "qualified-ident"
  def qualifiedIdentAll: MemParser[IdentAll] = qualifiedIdent <~ ".*" ^^ IdentAll named "ident-all"
  def variable: MemParser[Variable] = ((":" ~> ((ident | stringLiteral) ~
      rep("." ~> (ident | stringLiteral | wholeNumber)) ~ opt(":" ~> ident) ~ opt("?"))) | "?") ^^ {
    case "?" => Variable("?", null, null,  false)
    case (i: String) ~ (m: List[String @unchecked]) ~ (t: Option[String @unchecked]) ~ o => Variable(i, m, t orNull, o != None)
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
     * variable parser must be applied after query parser since ? mark matches variable
     * insert, delete, update parsers must be applied before query parser */
  def operand: MemParser[Any] = (TRUE | FALSE | ALL | withQuery | function | insert | update | query |
    decimalNr | stringLiteral | variable | id | idref | result | array) named "operand"
  def function: MemParser[Fun] = (qualifiedIdent <~ "(") ~ opt("#") ~ repsep(expr, ",") <~ ")" ^^ {
    case Ident(a) ~ d ~ b => Fun(a.mkString("."), b, d.map(x=> true).getOrElse(false))
  } named "function"
  def array: MemParser[Arr] = "[" ~> repsep(expr, ",") <~ "]" ^^ Arr named "array"

  //query parsers
  def join: MemParser[Join] = (("/" ~ opt("[" ~> expr <~ "]")) | (opt("[" ~> expr <~ "]") ~ "/") |
    ";" | array) ^^ {
      case ";" => NoJoin
      case "/" ~ Some(e) => Join(true, e, false)
      case "/" ~ None => DefaultJoin
      case Some(e) ~ "/" => Join(true, e, false)
      case None ~ "/" => DefaultJoin
      case a => Join(false, a, false)
    } named "join"
  def filter: MemParser[Arr] = array named "filter"
  def filters: MemParser[Filters] = rep(filter) ^^ Filters named "filters"
  def obj: MemParser[Obj] = opt(join) ~ opt("?") ~ (qualifiedIdent | braces) ~
    opt(opt("?" | "!") ~ ident ~ opt("?" | "!")) ^^ {
    case _ ~ Some(_) ~ _ ~ Some(Some(_) ~ _ ~ _ | _ ~ _ ~ Some(_)) =>
      sys.error("Cannot be right and left join at the same time")
    case join ~ rightoj ~ o ~ Some(leftoj ~ alias ~ leftoj1) =>
      Obj(o, alias, join.orNull,
        rightoj.map(x => "r") orElse (leftoj orElse leftoj1).map(j => if(j == "?") "l" else "i") orNull,
        (leftoj orElse leftoj1).map(_ == "?").getOrElse(false))
    case join ~ rightoj ~ o ~ None => Obj(o, null, join orNull, rightoj.map(x => "r") orNull)
  } named "obj"
  def objs: MemParser[List[Obj]] = obj ~ rep(obj ~ opt("?" | "!")) ^^ {
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
            List(o.copy(alias = (Option(a1).getOrElse(a)), join =
              j.copy(expr = o1.copy(alias = null, outerJoin = null, nullable = false)),
              outerJoin = (if (oj == null) oj1 else oj), nullable = n || n1)) ++
              (if (prevObj == null) List() else l.tail.flatMap {
                //flattenize array of foreign key shortcut joins
                case o2 @ Obj(_, a2, _, oj2, n2) => List((if (prevAlias != null) Obj(Ident(List(prevAlias)),
                  null, NoJoin, null, false)
                else Obj(prevObj.obj, null, NoJoin, null, false)),
                  o.copy(alias = (if (a2 != null) a2 else o.alias), join =
                    j.copy(expr = o2.copy(alias = null, outerJoin = null, nullable = false)),
                    outerJoin = (if (oj == null) oj2 else oj), nullable = n || n2))
                case x => List(o.copy(join = Join(false, x, false)))
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
  def column: MemParser[Col] = (qualifiedIdentAll |
    (expr ~ opt(":" ~> ident) ~ opt(stringLiteral | qualifiedIdent))) ^^ {
      case i: IdentAll => Col(i, null, null)
      //move object alias to column alias
      case (o @ Obj(_, a, _, _, _)) ~ (typ: Option[String @unchecked]) ~ None => Col(o.copy(alias = null), a, typ orNull)
      case e ~ (typ: Option[String @unchecked]) ~ (a: Option[_]) => Col(e, a map {
        case Ident(i) => i.mkString; case s => "\"" + s + "\""
      } orNull, typ orNull)
    } ^^ { pr =>
      def extractAlias(expr: Any): (String, Any) = expr match {
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
        case c @ Col(_: IdentAll | _: Obj, _, _) => c
        case c @ Col(e, null, _) => extractAlias(e) match {
          case (null, _) => c
          case (a, e) =>
            c.copy(col = e, alias = a)
        }
        case r => r
      }
    } named "column"
  def columns: MemParser[Cols] = (opt("#") <~ "{") ~ rep1sep(column, ",") <~ "}" ^^ {
    case d ~ c => Cols(d != None, c)
  } named "columns"
  def group: MemParser[Grp] = ("(" ~> rep1sep(expr, ",") <~ ")") ~
    opt(("^" ~ "(") ~> expr <~ ")") ^^ { case g ~ h => Grp(g, if (h == None) null else h.get) }  named "group"
  def orderMember: MemParser[(Exp, Any, Exp)] = opt(NULL) ~ expr ~ opt(NULL) ^^ {
    case Some(nf) ~ e ~ Some(nl) => sys.error("Cannot be nulls first and nulls last at the same time")
    case nf ~ e ~ nl => (if (nf == None) null else nf.get, e, if (nl == None) null else nl.get)
  } named "order-member"
  def order: MemParser[Ord] = ("#" ~ "(") ~> rep1sep(orderMember, ",") <~ ")" ^^ Ord named "order"
  def offsetLimit: MemParser[(Any, Any)] = ("@" ~ "(") ~> (wholeNumber | variable) ~ opt(",") ~
    opt(wholeNumber | variable) <~ ")" ^^ { pr =>
      {
        def c(x: Any) = x match { case v: Variable => v case s: String => BigDecimal(s) }
        pr match {
          case o ~ comma ~ Some(l) => (c(o), c(l))
          case o ~ Some(comma) ~ None => (c(o), null)
          case o ~ None ~ None => (null, c(o))
        }
      }
    } named "offset-limit"
  def query: MemParser[Any] = ((objs | NULL) ~ filters ~ opt(columns) ~ opt(group) ~ opt(order) ~
    opt(offsetLimit) | (columns ~ filters)) ^^ {
      case Null ~ Filters(Nil) ~ None ~ None ~ None ~ None => Null //null literal
      case List(t) ~ Filters(Nil) ~ None ~ None ~ None ~ None => t
      case t ~ (f: Filters) ~ (c: Option[Cols @unchecked]) ~ (g: Option[Grp @unchecked]) ~ (o: Option[Ord @unchecked]) ~
        (l: Option[(Any, Any) @unchecked]) => Query(t match {
          case tables: List[Obj @unchecked] => tables
          case Null => List(Obj(Null, null, null, null))
        }, f, c.orNull, g.orNull, o.orNull, l.map(_._1) orNull, l.map(_._2) orNull)
      case (c: Cols) ~ (f: Filters) => Query(List(Obj(Null, null, null, null)), f, c,
        null, null, null, null)
    } ^^ { pr =>
      def toDivChain(objs: List[Obj]): Any = objs match {
        case o :: Nil => o.obj
        case l => BinOp("/", l.head.obj, toDivChain(l.tail))
      }
      pr match {
        case Query(objs, Filters(Nil), null, null, null, null, null) if objs forall {
          case Obj(_, null, DefaultJoin, null, _) | Obj(_, null, null, null, _) => true
          case _ => false
        } => toDivChain(objs)
        case q => q
      }
    } named "query"

  def queryWithCols: MemParser[Any] = query ^? ({
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
  })

  def insert: MemParser[Insert] = (("+" ~> qualifiedIdent ~ opt(ident) ~ opt(columns) ~
    opt(queryWithCols | repsep(array, ","))) |
    ((qualifiedIdent ~ opt(ident) ~ opt(columns) <~ "+") ~ rep1sep(array, ","))) ^^ {
      case t ~ a ~ c ~ (v: Option[_]) => Insert(t, a.orNull, c.map(_.cols).orNull, v.orNull)
      case t ~ a ~ c ~ v => Insert(t, a.orNull, c.map(_.cols).orNull, v)
    } named "insert"
  def update: MemParser[Update] = (("=" ~> qualifiedIdent ~ opt(ident) ~
    opt(filter) ~ opt(columns) ~ opt(queryWithCols | array)) |
    ((qualifiedIdent ~ opt(ident) ~ opt(filter) ~ opt(columns) <~ "=") ~ array)) ^^ {
      case t ~ a ~ f ~ c ~ v => Update(t, a orNull, f orNull, c match {
        case Some(Cols(_, c)) => c
        case None => null
      }, v match {
        case a: Arr => a
        case Some(vals) => vals
        case None => null
      })
    } named "update"
  def delete: MemParser[Delete] = (("-" ~> qualifiedIdent ~ opt(ident) ~ filter) |
    (((qualifiedIdent ~ opt(ident)) <~ "-") ~ filter)) ^^ {
      case t ~ a ~ f => Delete(t, a orNull, f)
    } named "delete"
  //operation parsers
  //delete must be before alternative since it can start with - sign and
  //so it is not translated into minus expression!
  def unaryExpr: MemParser[Any] = delete | (opt("-" | "!" | "|" | "~") ~ operand) ^^ {
      case a ~ b => a.map(UnOp(_, b)).getOrElse(b)
      case x => x
    } named "unary-exp"
  def mulDiv: MemParser[Any] = unaryExpr ~ rep("*" ~ unaryExpr | "/" ~ unaryExpr) ^^ binOp named "mul-div"
  def plusMinus: MemParser[Any] = mulDiv ~ rep(("++" | "+" | "-" | "&&" | "||") ~ mulDiv) ^^ binOp named "plus-minus"
  def comp: MemParser[Any] = plusMinus ~ rep(comp_op ~ plusMinus) ^? (
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
    case lop ~ "in" ~ "(" ~ rop => In(lop, rop, false)
    case lop ~ "!in" ~ "(" ~ rop => In(lop, rop, true)
  } named "in"
  //in parser should come before comp so that it is not taken for in function which is illegal
  def logicalOp: MemParser[Any] = in | comp named "logical-op"
  def expr: MemParser[Any] = logicalOp ~ rep("&" ~ logicalOp | "|" ~ logicalOp) ^^ binOp named "expr"
  def exprList: MemParser[Any] = repsep(expr, ",") ^^ {
    case e :: Nil => e
    case l => Arr(l)
  } named "expr-list"

  private def binOp(p: ~[Any, List[~[String, Any]]]): Any = p match {
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
