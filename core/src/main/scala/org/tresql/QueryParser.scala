package org.tresql

import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.input.Reader
import sys._

object QueryParser extends JavaTokenParsers {

  trait Exp {
    def tresql:String
  }
  
  case class Ident(ident: List[String]) extends Exp {
    def tresql = ident.mkString(".")
  }
  case class Variable(variable: String, opt: Boolean) extends Exp {
    def tresql = (if (variable == "?") "?" else ":" + variable) + (if(opt) "?" else "")
  }
  case class Id(name: String) extends Exp {
    def tresql = "#" + name
  }
  case class IdRef(name: String) extends Exp {
    def tresql = ":#" + name
  }
  case class Result(rNr: Int, col: Any) extends Exp {
    def tresql = ":" + rNr + "(" + any2tresql(col) + ")"
  }
  case class UnOp(operation: String, operand: Any) extends Exp {
    def tresql = operation + any2tresql(operand)
  }
  case class Fun(name: String, parameters: List[Any]) extends Exp {
    def tresql = name + "(" + parameters.map(any2tresql(_)).mkString(",") + ")"
  }
  case class BinOp(op: String, lop: Any, rop: Any) extends Exp {
    def tresql = any2tresql(lop) + " " + op + " " + any2tresql(rop)
  }

  case class Join(default: Boolean, expr: Any, noJoin: Boolean) extends Exp {
    def tresql = this match {
      case Join(_, _, true) => ";"
      case Join(false, a:Arr, false) => a.tresql
      case Join(true, null, false) => "/"
      case Join(true, e, false) => "/[" + any2tresql(e) + "]"
    }
  }
  case class Obj(obj: Any, alias: String, join: Join, outerJoin: String) extends Exp {
    def tresql = (if(join != null) join.tresql else "") + (if (outerJoin == "r") "?" else "") +
      any2tresql(obj) + (if (outerJoin == "l") "?" else "") + (if(alias != null) " " + alias else "")
  }
  case class Col(col: Any, alias: String, typ: String) extends Exp {
    def tresql = any2tresql(col) + (if(typ != null) " :" + typ else "") +
      (if(alias != null) " " + alias else "")
  }
  case class Cols(distinct: Boolean, cols: List[Col]) extends Exp {
    def tresql = error("Not implemented")
  }
  case class Grp(cols: List[Any], having: Any) extends Exp {
    def tresql = "(" + cols.map(any2tresql(_)).mkString(",") + ")" +
      (if (having != null) "^(" + any2tresql(having) + ")" else "")
  }
  //cols expression is tuple in the form - ([<nulls first>], <order col list>, [<nulls last>])
  case class Ord(cols: (Null, List[Any], Null), asc: Boolean) extends Exp {
    def tresql = (if (asc) "#" else "~#") + "(" + (if(cols._1 == Null()) "null " else "") +
      cols._2.map(any2tresql(_)).mkString(",") + (if(cols._3 == Null()) " null" else "") + ")"
  }
  case class Query(tables: List[Obj], filter: Arr, cols: List[Col], distinct: Boolean,
    group: Grp, order: List[Ord], offset: Any, limit: Any) extends Exp {
    def tresql = tables.map(any2tresql(_)).mkString + 
      (if(filter != null) any2tresql(filter) else "") + (if(distinct) "#" else "") + 
      (if (cols != null) cols.map(_.tresql).mkString("{", ",", "}") else "") +
      (if(group != null) any2tresql(group) else "") +
      (if(order != null) order.map(_.tresql).mkString else "") + 
      (if(limit != null) "@(" + (if(offset != null) any2tresql(offset) + " " else "") + 
          any2tresql(limit) + ")" else "")
  }
  case class Insert(table: Ident, cols: List[Col], vals: List[Arr]) extends Exp {
    def tresql = "+" + table.tresql + cols.map(_.tresql).mkString("{", ",", "}") +
      vals.map(_.tresql).mkString(",")
  }
  case class Update(table: Ident, filter: Arr, cols: List[Col], vals: Arr) extends Exp {
    def tresql = "=" + table.tresql + (if(filter != null) filter.tresql else "") +
      cols.map(_.tresql).mkString("{", ",", "}") + vals.tresql
  }
  case class Delete(table: Ident, filter: Arr) extends Exp {
    def tresql = "-" + table.tresql + filter.tresql
  }
  case class Arr(elements: List[Any]) extends Exp {
    def tresql = elements.map(any2tresql(_)).mkString("[", ",", "]")
  }
  case class All() extends Exp {
    def tresql = "*"
  }
  case class IdentAll(ident: Ident) extends Exp {
    def tresql = ident.ident.mkString(".") + ".*"
  }
  case class Null() extends Exp {
    def tresql = "null"
  }

  case class Braces(expr: Any) extends Exp {
    def tresql = "(" + any2tresql(expr) + ")"
  }

  def quotedStringLiteral: Parser[String] = 
    ("'" + """([^'\p{Cntrl}\\]|\\[\\/bfnrt']|\\u[a-fA-F0-9]{4})*""" + "'").r ^^
      (s => s.substring(1, s.length - 1).replace("\\'", "'"))
  def doubleQuotedStringLiteral: Parser[String] =
    ("\"" + """([^"\p{Cntrl}\\]|\\[\\/bfnrt"]|\\u[a-fA-F0-9]{4})*""" + "\"").r ^^
      (s => s.substring(1, s.length - 1).replace("\\\"", "\""))
  override def stringLiteral: Parser[String] = quotedStringLiteral | doubleQuotedStringLiteral
  val KEYWORDS = Set("in", "null")
  def excludeKeywordsIdent = new Parser[String] {
    def apply(in: Input) = {
      ident(in) match {
        case s@Success(r, i) => if (!KEYWORDS.contains(r)) s 
            else Failure("illegal identifier: " + r, i)
        case r => r
      }
    }
  }
  def comment: Parser[String] = """/\*(.|[\r\n])*?\*/""".r
  def decimalNr: Parser[BigDecimal] = decimalNumber ^^ (BigDecimal(_))
  def TRUE: Parser[Boolean] = "true" ^^^ true
  def FALSE: Parser[Boolean] = "false" ^^^ false
  def NULL = "null" ^^^ Null()
  def ALL: Parser[All] = "*" ^^^ All()

  def qualifiedIdent: Parser[Ident] = rep1sep(excludeKeywordsIdent, ".") ^^ (Ident(_))
  def qualifiedIdentAll: Parser[IdentAll] = qualifiedIdent <~ ".*" ^^ (IdentAll(_))
  def variable: Parser[Variable] = ((":" ~> ((ident | stringLiteral) ~ opt("?"))) | "?") ^^ {
    case "?" => Variable("?", false)
    case (i: String) ~ o => Variable(i, o != None)
  }
  def id: Parser[Id] = "#" ~> ident ^^ (Id(_))
  def idref: Parser[IdRef] = ":#" ~> ident ^^ (IdRef(_))
  def result: Parser[Result] = (":" ~> "[0-9]+".r <~ "(") ~ ("[0-9]+".r | stringLiteral |
    qualifiedIdent) <~ ")" ^^ {
      case r ~ c => Result(r.toInt,
        c match {
          case s: String => try { s.toInt } catch { case _: NumberFormatException => s }
          case Ident(i) => i
        })
    }
  def bracesExp: Parser[Braces] = "(" ~> expr <~ ")" ^^ (Braces(_))
  /* Important is that function parser is applied before query because of the longest token
     * matching, otherwise qualifiedIdent of query will match earlier.
     * Also important is that bracesExpr parser is applied before query otherwise it will always 
     * match down to query.
     * Also important is that array parser is applied after query parser because it matches join parser
     * of the query.
     * Also important is that variable parser is after query parser since ? mark matches variable
     * Also important that insert, delete, update parsers are before query parser */
  def operand: Parser[Any] = (TRUE | FALSE | NULL | ALL | decimalNr |
    stringLiteral | function | insert | update | bracesExp | query | variable | id | 
    idref | result | array)
  def negation: Parser[UnOp] = "-" ~> operand ^^ (UnOp("-", _))
  def not: Parser[UnOp] = "!" ~> operand ^^ (UnOp("!", _))
  //is used within column clause to indicate separate query
  def sep: Parser[UnOp] = "|" ~> operand ^^ (UnOp("|", _))
  def function: Parser[Fun] = (ident <~ "(") ~ repsep(expr, ",") <~ ")" ^^ {
    case a ~ b => Fun(a, b)
  }
  def array: Parser[Arr] = "[" ~> repsep(expr, ",") <~ "]" ^^ (Arr(_))

  //query parsers
  def join: Parser[Join] = (("/" ~ opt("[" ~> expr <~ "]")) | (opt("[" ~> expr <~ "]") ~ "/") |
      ";" | array) ^^ {
    case ";" => Join(false, null, true)
    case "/" ~ Some(e) => Join(true, e, false)
    case "/" ~ None => Join(true, null, false)
    case Some(e) ~ "/" => Join(true, e, false)
    case None ~ "/" => Join(true, null, false)
    case a => Join(false, a, false)
  }
  def filter: Parser[Arr] = array
  def obj: Parser[Obj] = opt(join) ~ opt("?") ~ (qualifiedIdent | bracesExp) ~
    opt("?") ~ opt(excludeKeywordsIdent) ^^ {
      case a ~ Some(b) ~ c ~ Some(d) ~ e => error("Cannot be right and left join at the same time")
      case a ~ b ~ c ~ d ~ e => Obj(c, e.orNull, a.orNull,
          if (b != None) "r" else if (d != None) "l" else null)
    }
  def objs: Parser[List[Obj]] = rep1(obj)
  def column: Parser[Col] = (qualifiedIdentAll |
      (expr ~ opt(":" ~> ident) ~ opt(stringLiteral | qualifiedIdent))) ^^ {
    case i:IdentAll => Col(i, null, null)
    case (o@Obj(_, a, _, _)) ~ (typ: Option[String]) ~ None => Col(o, a, typ orNull)
    case e ~ (typ: Option[String]) ~ (a: Option[_]) => Col(e, a map { 
      case Ident(i) => i.mkString; case s => "\"" + s + "\"" } orNull, typ orNull)
  }
  def columns: Parser[Cols] = (opt("#") <~ "{") ~ rep1sep(column, ",") <~ "}" ^^ {
    case d ~ c => Cols(d != None, c)
  }
  def group: Parser[Grp] = ("(" ~> rep1sep(expr, ",") <~ ")") ~
    opt(("^" ~ "(") ~> expr <~ ")") ^^ { case g ~ h => Grp(g, if (h == None) null else h.get) }
  def orderCore: Parser[(Null, List[Any], Null)] = opt(NULL) ~ rep1sep(expr, ",") ~ opt(NULL) ^^ {
    case Some(nf) ~ e ~ Some(nl) => error("Cannot be nulls first and nulls last at the same time")
    case nf ~ e ~ nl => (if(nf == None) null else nf.get, e, if(nl == None) null else nl.get)
  }
  def orderAsc: Parser[Ord] = ("#" ~ "(") ~> orderCore <~ ")" ^^ (Ord(_, true))
  def orderDesc: Parser[Ord] = ("~#" ~ "(") ~> orderCore <~ ")" ^^ (Ord(_, false))
  def order: Parser[List[Ord]] = rep1(orderAsc | orderDesc)
  def offsetLimit: Parser[(Any, Any)] = ("@" ~ "(") ~> ("[0-9]+".r | variable) ~
    opt("[0-9]+".r | variable) <~ ")" ^^ {
      case o ~ l =>
        if (l == None) (null, o match { case v: Variable => v case s: String => BigDecimal(s) })
        else (o match { case v: Variable => v case s: String => BigDecimal(s) },
          l match {
            case Some(v: Variable) => v case Some(s: String) => BigDecimal(s)
            case None => error("Knipis")
          })
    }
  def query: Parser[Any] = objs ~ opt(filter) ~ opt(columns) ~ opt(group) ~ opt(order) ~
    opt(offsetLimit) ^^ {
      case (t :: Nil) ~ None ~ None ~ None ~ None ~ None => t
      case t ~ f ~ c ~ g ~ o ~ l => Query(t, f.orNull, c.map(_.cols) orNull,
        c.map(_.distinct) getOrElse false, g.orNull, o.orNull, l.map(_._1) orNull, l.map(_._2) orNull)
    }
  def insert: Parser[Insert] = (("+" ~> qualifiedIdent ~ columns ~ rep1sep(array, ",")) | 
      ((qualifiedIdent ~ columns <~ "+") ~ rep1sep(array, ","))) ^^ {
    case t ~ Cols(_, c) ~ v => Insert(t, c, v)
  }
  def update: Parser[Update] = (("=" ~> qualifiedIdent ~ opt(filter) ~ columns ~ array) | 
      ((qualifiedIdent ~ opt(filter) ~ columns <~ "=") ~ array)) ^^ {
    case t ~ f ~ Cols(_, c) ~ v => Update(t, f orNull, c, v)
  }
  def delete: Parser[Delete] = (("-" ~> qualifiedIdent ~ filter) | ((qualifiedIdent <~ "-") ~ filter)) ^^ {
    case t ~ f => Delete(t, f)
  }
  //operation parsers
  //delete must be before negation since it can start with - sign!
  def unaryExpr = delete | negation | not | operand | sep
  def mulDiv: Parser[Any] = unaryExpr ~ rep("*" ~ unaryExpr | "/" ~ unaryExpr) ^^ (p => binOp(p))
  def plusMinus: Parser[Any] = mulDiv ~ rep(("++" | "+" | "-" | "&&" | "||") ~ mulDiv) ^^ (p => binOp(p))
  def comp: Parser[Any] = plusMinus ~
    opt(("<=" | ">=" | "<" | ">" | "!=" | "=" | "~~" | "!~~" | "~" | "!~" | "in" | "!in") ~ plusMinus) ^^ {
      case l ~ None => l
      case l ~ Some(o ~ r) => BinOp(o, l, r)
    }
  def logical: Parser[Any] = comp ~ rep("&" ~ comp | "|" ~ comp) ^^ (p => binOp(p))

  //expression
  def expr: Parser[Any] = opt(comment) ~> logical <~ opt(comment)

  def exprList: Parser[Any] = repsep(expr, ",") ^^ {
    case e :: Nil => e
    case l => Arr(l)
  }

  def parseAll(expr: String): ParseResult[Any] = {
    parseAll(exprList, expr)
  }
  
  def parseExp(expr:String):Any = {
    parseAll(expr) match {
      case Success(r, _) => r
      case x => error(x.toString)
    }    
  }

  private def binOp(p: ~[Any, List[~[String, Any]]]): Any = p match {
    case e ~ Nil => e
    case e ~ (l@((o ~ _) :: _)) => BinOp(o, e, binOp(l))
  }

  private def binOp(l: List[~[String, Any]]): Any = l match {
    case (_ ~ e) :: Nil => e
    case (_ ~ e) :: (l@((o ~ _) :: _)) => BinOp(o, e, binOp(l))
    case _ => error("Knipis")
  }
  
  def any2tresql(any:Any) = any match {
    case a:String => if (a.contains("'")) "\"" + a + "\"" else "'" + a + "'"
    case e:Exp => e.tresql
    case null => "null"
    case x => x.toString
  }

  def bindVariables(ex: String): List[String] = {
    var bindIdx = 0
    val vars = scala.collection.mutable.ListBuffer[String]()
    def bindVars(parsedExpr: Any): Any =
      parsedExpr match {
        case Variable("?", _) => bindIdx += 1; vars += bindIdx.toString
        case Variable(n, _) => vars += n
        case Fun(_, pars) => pars foreach bindVars
        case UnOp(_, operand) => bindVars(operand)
        case BinOp(_, lop, rop) => bindVars(lop); bindVars(rop)
        case Obj(t, _, j, _) => bindVars(j); bindVars(t)
        case Col(c, _, _) => bindVars(c)
        case Cols(_, cols) => cols foreach bindVars
        case Grp(cols, hv) => cols foreach bindVars; bindVars(hv)
        case Ord(cols, _) => cols._2 foreach bindVars
        case Query(objs, filter, cols, _, gr, ord, _, _) => {
          objs foreach bindVars; bindVars(filter)
          if (cols != null) cols foreach bindVars
          bindVars(gr)
          if (ord != null) ord foreach bindVars
        }
        case Insert(_, cols, vals) => {
          cols foreach bindVars
          vals foreach bindVars
        }
        case Update(_, filter, cols, vals) => {
          if (filter != null) bindVars(filter)
          cols foreach bindVars
          bindVars(vals)          
        }
        case Delete(_, filter) => bindVars(filter)
        case Arr(els) => els foreach bindVars
        case Braces(expr) => bindVars(expr)
        case _ =>
      }
    parseAll(ex) match {
      case Success(r, _) => {
        bindVars(r)
        vars.toList
      }
      case x => error(x.toString)
    }

  }

  def main(args: Array[String]) {
    args.length match {
      case 0 => println("usage: <string to parse>")
      case 1 => {
        println("parsing: " + args(0))
        println(parse(expr, args(0)))
      }
    }
  }
}