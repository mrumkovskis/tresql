package org.tresql.parsing

import scala.util.parsing.combinator.syntactical.StandardTokenParsers

trait QueryParsers extends StandardTokenParsers {

  lexical.reserved ++= Set("in", "null", "true", "false", "<=", ">=", "<", ">", "!=", "=", "~~",
    "!~~", "~", "!~", "in", "!in", "++", "+", "-", "&&", "||", "&", "|", "*", "/", "!", "?", ".*")
  //for lexical scanner to pass all reserved words must also be in delimiter set
  lexical.delimiters ++= (lexical.reserved ++ 
      Set("(", ")", "[", "]", "{", "}", "#", "@", "^", ",", ".", ":", ":#", ";"))

  trait Exp {
    def tresql: String
  }

  case class Ident(ident: List[String]) extends Exp {
    def tresql = ident.mkString(".")
  }
  case class Variable(variable: String, opt: Boolean) extends Exp {
    def tresql = (if (variable == "?") "?" else ":") +
      (if (variable contains "'") "\"" + variable + "\"" else "'" + variable + "'") +
      (if (opt) "?" else "")
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
      ((parameters map any2tresql) mkString ",") + ")"
  }
  case class In(lop: Any, rop: List[Any], not: Boolean) extends Exp {
    def tresql = any2tresql(lop) + (if (not) " not" else "") + rop.map(any2tresql).mkString(
      " in(", ", ", ")")
  }
  case class BinOp(op: String, lop: Any, rop: Any) extends Exp {
    def tresql = any2tresql(lop) + " " + op + " " + any2tresql(rop)
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
      any2tresql(c._2) + (if (c._1 == Null) "null " else "")).mkString(",") + ")"
  }
  case class Query(tables: List[Obj], filter: Filters, cols: List[Col], distinct: Boolean,
    group: Grp, order: Ord, offset: Any, limit: Any) extends Exp {
    def tresql = tables.map(any2tresql).mkString +
      filter.tresql + (if (distinct) "#" else "") +
      (if (cols != null) cols.map(_.tresql).mkString("{", ",", "}") else "") +
      (if (group != null) any2tresql(group) else "") +
      (if (order != null) order.tresql else "") +
      (if (limit != null) "@(" + (if (offset != null) any2tresql(offset) + " " else "") +
        any2tresql(limit) + ")"
      else "")
  }
  case class Insert(table: Ident, alias: String, cols: List[Col], vals: Any) extends Exp {
    def tresql = "+" + table.tresql + Option(alias).getOrElse("") +
      cols.map(_.tresql).mkString("{", ",", "}") + any2tresql(vals)
  }
  case class Update(table: Ident, alias: String, filter: Arr, cols: List[Col], vals: Any) extends Exp {
    def tresql = "=" + table.tresql + Option(alias).getOrElse("") +
      (if (filter != null) filter.tresql else "") +
      cols.map(_.tresql).mkString("{", ",", "}") + any2tresql(vals)
  }
  case class Delete(table: Ident, alias: String, filter: Arr) extends Exp {
    def tresql = "-" + table.tresql + Option(alias).getOrElse("") + filter.tresql
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
  def decimalNr = ((numericLit ~ opt("." ~ numericLit)) | (opt(numericLit) ~ "." ~ numericLit)) ^^ {
    case (n: String) ~ None => BigDecimal(n)
    case n ~ Some("." ~ d) => BigDecimal(n + "." + d)
    case None ~ "." ~ d => BigDecimal("." + d)
  }
  def TRUE = "true" ^^^ true
  def FALSE = "false" ^^^ false
  def NULL = "null" ^^^ Null
  def ALL: Parser[All] = "*" ^^^ All()

  def qualifiedIdent = rep1sep(ident, ".") ^^ Ident
  def qualifiedIdentAll = qualifiedIdent <~ ".*" ^^ IdentAll
  def variable = ((":" ~> ((ident | stringLit) ~ opt("?"))) | "?") ^^ {
    case "?" => Variable("?", false)
    case (i: String) ~ o => Variable(i, o != None)
  }
  def id = "#" ~> ident ^^ Id
  def idref = ":#" ~> ident ^^ IdRef
  def result: Parser[Res] = (":" ~> numericLit <~ "(") ~ (numericLit | stringLit |
    qualifiedIdent) <~ ")" ^^ {
      case r ~ c => Res(r.toInt,
        c match {
          case s: String => try { s.toInt } catch { case _: NumberFormatException => s }
          case Ident(i) => i
        })
    }
  def bracesExp: Parser[Braces] = "(" ~> expr <~ ")" ^^ Braces
  /* Important is that function parser is applied before query because of the longest token
     * matching, otherwise qualifiedIdent of query will match earlier.
     * Also important is that array parser is applied after query parser because it matches join parser
     * of the query.
     * Also important is that variable parser is after query parser since ? mark matches variable
     * Also important that insert, delete, update parsers are before query parser */
  def operand: Parser[Any] = (TRUE | FALSE | NULL | ALL | function | insert | update | query |
    decimalNr | stringLit | variable | id | idref | result | array)
  def negation: Parser[UnOp] = "-" ~> operand ^^ (UnOp("-", _))
  def not: Parser[UnOp] = "!" ~> operand ^^ (UnOp("!", _))
  //is used within column clause to indicate separate query
  def sep: Parser[UnOp] = "|" ~> operand ^^ (UnOp("|", _))
  //is used in order by desc
  def desc: Parser[UnOp] = "~" ~> operand ^^ (UnOp("~", _))
  def function: Parser[Fun] = (qualifiedIdent <~ "(") ~ opt("#") ~ repsep(expr, ",") <~ ")" ^^ {
    case Ident(a) ~ d ~ b => Fun(a.mkString("."), b, d.map(x=> true).getOrElse(false))
  }
  def array: Parser[Arr] = "[" ~> repsep(expr, ",") <~ "]" ^^ Arr

  //query parsers
  def join: Parser[Join] = (("/" ~ opt("[" ~> expr <~ "]")) | (opt("[" ~> expr <~ "]") ~ "/") |
    ";" | array) ^^ {
      case ";" => NoJoin
      case "/" ~ Some(e) => Join(true, e, false)
      case "/" ~ None => DefaultJoin
      case Some(e) ~ "/" => Join(true, e, false)
      case None ~ "/" => DefaultJoin
      case a => Join(false, a, false)
    }
  def filter: Parser[Arr] = array
  def filters: Parser[Filters] = rep(filter) ^^ Filters
  def obj: Parser[Obj] = opt(join) ~ opt("?") ~ (qualifiedIdent | bracesExp) ~
    opt("?") ~ opt(ident) ~ opt("?") ^^ {
      case a ~ Some(b) ~ c ~ Some(d) ~ e ~ f => sys.error("Cannot be right and left join at the same time")
      case a ~ Some(b) ~ c ~ d ~ e ~ Some(f) => sys.error("Cannot be right and left join at the same time")
      case a ~ b ~ c ~ d ~ e ~ f => Obj(c, e.orNull, a.orNull,
        b.map(x => "r") orElse d.orElse(f).map(x => "l") orNull,
        //set nullable flag if left outer join
        d orElse f map (x => true) getOrElse (false))
    }
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
  }
  def column = (qualifiedIdentAll |
    (expr ~ opt(":" ~> ident) ~ opt(stringLit | qualifiedIdent))) ^^ {
      case i: IdentAll => Col(i, null, null)
      case (o @ Obj(_, a, _, _, _)) ~ (typ: Option[String]) ~ None => Col(o, a, typ orNull)
      case e ~ (typ: Option[String]) ~ (a: Option[_]) => Col(e, a map {
        case Ident(i) => i.mkString; case s => "\"" + s + "\""
      } orNull, typ orNull)
    } ^^ { pr =>
      def extractAlias(expr: Any): String = expr match {
        case BinOp(_, lop, rop) => extractAlias(rop)
        case UnOp(_, op) => extractAlias(op)
        case Obj(_, alias, _, null, _) => alias
        case _ => null
      }
      pr match {
        case c @ Col(_: IdentAll | _: Obj, _, _) => c
        case c @ Col(e, null, _) => extractAlias(e) match {
          case null => c
          case x => c.copy(alias = x)
        }
        case r => r
      }
    }
  def columns: Parser[Cols] = (opt("#") <~ "{") ~ rep1sep(column, ",") <~ "}" ^^ {
    case d ~ c => Cols(d != None, c)
  }
  def group: Parser[Grp] = ("(" ~> rep1sep(expr, ",") <~ ")") ~
    opt(("^" ~ "(") ~> expr <~ ")") ^^ { case g ~ h => Grp(g, if (h == None) null else h.get) }
  def orderMember: Parser[(Exp, Any, Exp)] = opt(NULL) ~ expr ~ opt(NULL) ^^ {
    case Some(nf) ~ e ~ Some(nl) => sys.error("Cannot be nulls first and nulls last at the same time")
    case nf ~ e ~ nl => (if (nf == None) null else nf.get, e, if (nl == None) null else nl.get)
  }
  def order: Parser[Ord] = ("#" ~ "(") ~> rep1sep(orderMember, ",") <~ ")" ^^ Ord 
  def offsetLimit: Parser[(Any, Any)] = ("@" ~ "(") ~> (numericLit | variable) ~ opt(",") ~
    opt(numericLit | variable) <~ ")" ^^ { pr =>
      {
        def c(x: Any) = x match { case v: Variable => v case s: String => BigDecimal(s) }
        pr match {
          case o ~ comma ~ Some(l) => (c(o), c(l))
          case o ~ Some(comma) ~ None => (c(o), null)
          case o ~ None ~ None => (null, c(o))
        }
      }
    }
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
    }
      
  def queryWithCols: Parser[Any] = query ^? ({
      case q @ Query(objs, _, cols, _, _, _, _, _) if cols != null => q
    }, {case x => "Query must contain column clause: " + x})

  def insert: Parser[Insert] = (("+" ~> qualifiedIdent ~ opt(ident) ~ opt(columns) ~
    opt(queryWithCols | repsep(array, ","))) |
    ((qualifiedIdent ~ opt(ident) ~ opt(columns) <~ "+") ~ rep1sep(array, ","))) ^^ {
      case t ~ a ~ c ~ (v: Option[_]) => Insert(t, a.orNull, c.map(_.cols).orNull, v.orNull)
      case t ~ a ~ c ~ v => Insert(t, a.orNull, c.map(_.cols).orNull, v)
    }
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
    }
  def delete: Parser[Delete] = (("-" ~> qualifiedIdent ~ opt(ident) ~ filter) |
    (((qualifiedIdent ~ opt(ident)) <~ "-") ~ filter)) ^^ {
      case t ~ a ~ f => Delete(t, a orNull, f)
    }
  //operation parsers
  //delete must be before negation since it can start with - sign and
  //before operand so it is not translated into minus expression!
  def unaryExpr = delete | operand | negation | not | sep | desc
  def mulDiv: Parser[Any] = unaryExpr ~ rep("*" ~ unaryExpr | "/" ~ unaryExpr) ^^ binOp
  def plusMinus: Parser[Any] = mulDiv ~ rep(("++" | "+" | "-" | "&&" | "||") ~ mulDiv) ^^ binOp
  def comp: Parser[Any] = plusMinus ~
    rep(("<=" | ">=" | "<" | ">" | "!=" | "=" | "~~" | "!~~" | "~" | "!~" | "in" | "!in") ~ plusMinus) ^? (
      {
        case lop ~ Nil => lop
        case lop ~ ((o ~ rop) :: Nil) => BinOp(o, lop, rop)
        case lop ~ List((o1 ~ mop), (o2 ~ rop)) => BinOp("&", BinOp(o1, lop, mop), BinOp(o2, mop, rop))
      },
      {
        case lop ~ x => "Ternary comparison operation is allowed, however, here " + (x.size + 1) +
          " operands encountered."
      })
  def in: Parser[In] = plusMinus ~ ("in" | "!in") ~ "(" ~ rep1sep(plusMinus, ",") <~ ")" ^^ {
    case lop ~ "in" ~ "(" ~ rop => In(lop, rop, false)
    case lop ~ "!in" ~ "(" ~ rop => In(lop, rop, true)
  }
  //in parser should come before comp so that it is not taken for in function which is illegal
  def logicalOp = in | comp
  def expr: Parser[Any] = logicalOp ~ rep("&" ~ logicalOp | "|" ~ logicalOp) ^^ binOp
  def exprList: Parser[Any] = repsep(expr, ",") ^^ {
    case e :: Nil => e
    case l => Arr(l)
  }
  
  private def binOp(p: ~[Any, List[~[String, Any]]]): Any = p match {
    case lop ~ Nil => lop
    case lop ~ ((o ~ rop) :: l) => BinOp(o, lop, binOp(this.~(rop, l)))
  }  
}

object QueryParsers extends QueryParsers
