package org.tresql.parsing

import scala.util.parsing.combinator.JavaTokenParsers

trait QueryParsers extends JavaTokenParsers {

  val reserved = Set("in")

  //comparison operand regular expression
  val comp_op = """!?in|[<>=!~%$]+"""r  
  
  //JavaTokenParsers overrides
  //besides standart whitespace symbols consider as a whitespace also comments in form /* comment */ and //comment 
  override val whiteSpace = """\s*+(/\*(.|\s)*?\*/\s*+)?(//(.)*+(\n|$))?"""r
  
  override def stringLiteral: Parser[String] = ("""("[^"]*+")|('[^']*+')"""r) ^^ {
    case s => s.substring(1, s.length - 1)
  } named "string-literal"

  override def ident = super.ident ^? ({ case x if !(reserved contains x) => x }, 
      { x => s"'$x' is one of the reserved words: $reserved" }) named "ident"
  
  override def wholeNumber: Parser[String] = ("""\d+"""r) named "number"
      
  def decimalNr = decimalNumber ^^ (BigDecimal(_)) named "decimal-nr"
  
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
      
  sealed trait Exp {
    def tresql: String
  }    

  case class Ident(ident: List[String]) extends Exp {
    def tresql = ident.mkString(".")
  }
  case class Variable(variable: String, typ: String, opt: Boolean) extends Exp {
    def tresql = if (variable == "?") "?" else {
      ":" +
        (if (variable contains "'") "\"" + variable + "\"" else "'" + variable + "'") +
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
      case Join(false, e: Exp, false) => "[" + e.tresql + "]"
      case Join(true, null, false) => "/"
      case Join(true, e, false) => "/[" + any2tresql(e) + "]"
    }
  }
  val NoJoin = Join(false, null, true)
  val DefaultJoin = Join(true, null, false)

  case class Obj(obj: Any, alias: String, join: Join, outerJoin: String, nullable: Boolean) extends Exp {
    def tresql = (if (join != null) join.tresql else "") + (if (outerJoin == "r") "?" else "") +
      any2tresql(obj) + (if (outerJoin == "l") "?" else "") + (if (alias != null) " " + alias else "")
  }
  case class Col(col: Any, alias: String, typ: String) extends Exp {
    def tresql = any2tresql(col) + (if (typ != null) " :" + typ else "") +
      (if (alias != null) " " + alias else "")
  }
  case class Cols(distinct: Boolean, cols: List[Col]) extends Exp {
    def tresql = sys.error("Not implemented")
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
  case class Query(tables: List[Obj], filter: Filters, cols: List[Col], distinct: Boolean,
    group: Grp, order: Ord, offset: Any, limit: Any) extends Exp {
    def tresql = tables.map(any2tresql).mkString +
      filter.tresql + (if (distinct) "#" else "") +
      (if (cols != null) cols.map(_.tresql).mkString("{", ",", "}") else "") +
      (if (group != null) any2tresql(group) else "") +
      (if (order != null) order.tresql else "") +
      (if (limit != null)
        s"@(${(if (offset != null) any2tresql(offset) + " " else "")}${any2tresql(limit)})"
      else if (offset != null) s"@(${any2tresql(offset)}, )"
      else "")
  }
  case class Insert(table: Ident, alias: String, cols: List[Col], vals: Any) extends Exp {
    def tresql = "+" + table.tresql + Option(alias).map(" " + _).getOrElse("") +
      (if (cols != null) cols.map(_.tresql).mkString("{", ",", "}") else "") +
      (if (vals != null) any2tresql(vals) else "")
  }
  case class Update(table: Ident, alias: String, filter: Arr, cols: List[Col], vals: Any) extends Exp {
    def tresql = "=" + table.tresql + Option(alias).map(" " + _).getOrElse("") +
      (if (filter != null) filter.tresql else "") +
      (if (cols != null) cols.map(_.tresql).mkString("{", ",", "}") else "") +
      (if (vals != null) any2tresql(vals) else "")
  }
  case class Delete(table: Ident, alias: String, filter: Arr) extends Exp {
    def tresql = "-" + table.tresql + Option(alias).map(" " + _).getOrElse("") + filter.tresql
  }
  case class Arr(elements: List[Any]) extends Exp {
    def tresql = "[" + any2tresql(elements) + "]"
  }
  case class Filters(filters: List[Arr]) extends Exp {
    def tresql = filters map any2tresql mkString
  }
  case class All() extends Exp {
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
    case e: Exp => e.tresql
    case null => "null"
    case l: List[_] => l map any2tresql mkString ", "
    case x => x.toString
  }
  
  //literals
  def TRUE: Parser[Boolean] = "true" ^^^ true named "true"
  def FALSE: Parser[Boolean] = "false" ^^^ false named "false"
  def NULL: Parser[Null.type] = "null" ^^^ Null named "null"
  def ALL: Parser[All] = "*" ^^^ All() named "all"

  def qualifiedIdent: Parser[Ident] = rep1sep(ident, ".") ^^ Ident named "qualified-ident"
  def qualifiedIdentAll: Parser[IdentAll] = qualifiedIdent <~ ".*" ^^ IdentAll named "ident-all"
  def variable: Parser[Variable] = ((":" ~> ((ident | stringLiteral) ~ opt(":" ~> ident) ~ opt("?")))
      | "?") ^^ {
    case "?" => Variable("?", null, false)
    case (i: String) ~ (t: Option[String]) ~ o => Variable(i, t orNull, o != None)
  } named "variable"
  def id: Parser[Id] = "#" ~> ident ^^ Id named "id"
  def idref: Parser[IdRef] = ":#" ~> ident ^^ IdRef named "id-ref"
  def result: Parser[Res] = (":" ~> wholeNumber <~ "(") ~ (wholeNumber | stringLiteral |
    qualifiedIdent) <~ ")" ^^ {
      case r ~ c => Res(r.toInt,
        c match {
          case s: String => try { s.toInt } catch { case _: NumberFormatException => s }
          case i: Ident => i
        })
    } named "result"
  def braces: Parser[Braces] = "(" ~> expr <~ ")" ^^ Braces named "braces"
  /* Important is that function parser is applied before query because of the longest token
     * matching, otherwise qualifiedIdent of query will match earlier.
     * Also important is that array parser is applied after query parser because it matches join parser
     * of the query.
     * Also important is that variable parser is after query parser since ? mark matches variable
     * Also important that insert, delete, update parsers are before query parser */
  def operand: Parser[Any] = (TRUE | FALSE | NULL | ALL | function | insert | update | query |
    decimalNr | stringLiteral | variable | id | idref | result | array) named "operand"
  def function: Parser[Fun] = (qualifiedIdent <~ "(") ~ opt("#") ~ repsep(expr, ",") <~ ")" ^^ {
    case Ident(a) ~ d ~ b => Fun(a.mkString("."), b, d.map(x=> true).getOrElse(false))
  } named "function"
  def array: Parser[Arr] = "[" ~> repsep(expr, ",") <~ "]" ^^ Arr named "array"

  //query parsers
  def join: Parser[Join] = (("/" ~ opt("[" ~> expr <~ "]")) | (opt("[" ~> expr <~ "]") ~ "/") |
    ";" | array) ^^ {
      case ";" => NoJoin
      case "/" ~ Some(e) => Join(true, e, false)
      case "/" ~ None => DefaultJoin
      case Some(e) ~ "/" => Join(true, e, false)
      case None ~ "/" => DefaultJoin
      case a => Join(false, a, false)
    } named "join"
  def filter: Parser[Arr] = array named "filter"
  def filters: Parser[Filters] = rep(filter) ^^ Filters named "filters"
  def obj: Parser[Obj] = opt(join) ~ opt("?") ~ (qualifiedIdent | braces) ~
    opt("?") ~ opt(ident) ~ opt("?") ^^ {
      case a ~ Some(b) ~ c ~ Some(d) ~ e ~ f => sys.error("Cannot be right and left join at the same time")
      case a ~ Some(b) ~ c ~ d ~ e ~ Some(f) => sys.error("Cannot be right and left join at the same time")
      case a ~ b ~ c ~ d ~ e ~ f => Obj(c, e.orNull, a.orNull,
        b.map(x => "r") orElse d.orElse(f).map(x => "l") orNull,
        //set nullable flag if left outer join
        d orElse f map (x => true) getOrElse (false))
    } named "obj"
  def objs: Parser[List[Obj]] = rep1(obj) ^^ { l =>
    var prev: Obj = null
    val res = l.flatMap { thisObj =>
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
  def column: Parser[Col] = (qualifiedIdentAll |
    (expr ~ opt(":" ~> ident) ~ opt(stringLiteral | qualifiedIdent))) ^^ {
      case i: IdentAll => Col(i, null, null)
      //move object alias to column alias
      case (o @ Obj(_, a, _, _, _)) ~ (typ: Option[String]) ~ None => Col(o.copy(alias = null), a, typ orNull)
      case e ~ (typ: Option[String]) ~ (a: Option[_]) => Col(e, a map {
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
  def columns: Parser[Cols] = (opt("#") <~ "{") ~ rep1sep(column, ",") <~ "}" ^^ {
    case d ~ c => Cols(d != None, c)
  } named "columns"
  def group: Parser[Grp] = ("(" ~> rep1sep(expr, ",") <~ ")") ~
    opt(("^" ~ "(") ~> expr <~ ")") ^^ { case g ~ h => Grp(g, if (h == None) null else h.get) }  named "group"
  def orderMember: Parser[(Exp, Any, Exp)] = opt(NULL) ~ expr ~ opt(NULL) ^^ {
    case Some(nf) ~ e ~ Some(nl) => sys.error("Cannot be nulls first and nulls last at the same time")
    case nf ~ e ~ nl => (if (nf == None) null else nf.get, e, if (nl == None) null else nl.get)
  } named "order-member"
  def order: Parser[Ord] = ("#" ~ "(") ~> rep1sep(orderMember, ",") <~ ")" ^^ Ord named "order"
  def offsetLimit: Parser[(Any, Any)] = ("@" ~ "(") ~> (wholeNumber | variable) ~ opt(",") ~
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
  def query: Parser[Any] = objs ~ filters ~ opt(columns) ~ opt(group) ~ opt(order) ~
    opt(offsetLimit) ^^ {
      case (t :: Nil) ~ Filters(Nil) ~ None ~ None ~ None ~ None => t
      case t ~ f ~ c ~ g ~ o ~ l => Query(t, f, c.map(_.cols) orNull,
        c.map(_.distinct) getOrElse false, g.orNull, o.orNull, l.map(_._1) orNull, l.map(_._2) orNull)
    } ^^ { pr =>
      def toDivChain(objs: List[Obj]): Any = objs match {
        case o :: Nil => o.obj
        case l => BinOp("/", l.head.obj, toDivChain(l.tail))
      }
      pr match {
        case Query(objs, Filters(Nil), null, false, null, null, null, null) if objs forall {
          case Obj(_, null, DefaultJoin, null, _) | Obj(_, null, null, null, _) => true
          case _ => false
        } => toDivChain(objs)
        case q => q
      }
    }  named "query"
      
  def queryWithCols: Parser[Any] = query ^? ({
      case q @ Query(objs, _, cols, _, _, _, _, _) if cols != null => q
    }, {case x => "Query must contain column clause: " + x}) named "query-with-cols"

  def insert: Parser[Insert] = (("+" ~> qualifiedIdent ~ opt(ident) ~ opt(columns) ~
    opt(queryWithCols | repsep(array, ","))) |
    ((qualifiedIdent ~ opt(ident) ~ opt(columns) <~ "+") ~ rep1sep(array, ","))) ^^ {
      case t ~ a ~ c ~ (v: Option[_]) => Insert(t, a.orNull, c.map(_.cols).orNull, v.orNull)
      case t ~ a ~ c ~ v => Insert(t, a.orNull, c.map(_.cols).orNull, v)
    } named "insert"
  def update: Parser[Update] = (("=" ~> qualifiedIdent ~ opt(ident) ~
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
  def delete: Parser[Delete] = (("-" ~> qualifiedIdent ~ opt(ident) ~ filter) |
    (((qualifiedIdent ~ opt(ident)) <~ "-") ~ filter)) ^^ {
      case t ~ a ~ f => Delete(t, a orNull, f)
    } named "delete"
  //operation parsers
  //delete must be before alternative since it can start with - sign and
  //so it is not translated into minus expression!
  def unaryExpr: Parser[Any] = delete | (opt("-" | "!" | "|" | "~") ~ operand) ^^ {
      case a ~ b => a.map(UnOp(_, b)).getOrElse(b)
      case x => x
    } named "unary-exp"
  def mulDiv: Parser[Any] = unaryExpr ~ rep("*" ~ unaryExpr | "/" ~ unaryExpr) ^^ binOp named "mul-div"
  def plusMinus: Parser[Any] = mulDiv ~ rep(("++" | "+" | "-" | "&&" | "||") ~ mulDiv) ^^ binOp named "plus-minus"
  def comp: Parser[Any] = plusMinus ~ rep(comp_op ~ plusMinus) ^? (
      {
        case lop ~ Nil => lop
        case lop ~ ((o ~ rop) :: Nil) => BinOp(o, lop, rop)
        case lop ~ List((o1 ~ mop), (o2 ~ rop)) => TerOp(lop, o1, mop, o2, rop)
      },
      {
        case lop ~ x => "Ternary comparison operation is allowed, however, here " + (x.size + 1) +
          " operands encountered."
      }) named "comp"
  def in: Parser[In] = plusMinus ~ ("in" | "!in") ~ "(" ~ rep1sep(plusMinus, ",") <~ ")" ^^ {
    case lop ~ "in" ~ "(" ~ rop => In(lop, rop, false)
    case lop ~ "!in" ~ "(" ~ rop => In(lop, rop, true)
  } named "in"
  //in parser should come before comp so that it is not taken for in function which is illegal
  def logicalOp: Parser[Any] = in | comp named "logical-op"
  def expr: Parser[Any] = logicalOp ~ rep("&" ~ logicalOp | "|" ~ logicalOp) ^^ binOp named "expr"
  def exprList: Parser[Any] = repsep(expr, ",") ^^ {
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

