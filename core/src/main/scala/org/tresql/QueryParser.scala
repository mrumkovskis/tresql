package org.tresql

import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.input.Reader
import sys._

object QueryParser extends JavaTokenParsers {

  case class Ident(ident: List[String])
  case class Variable(variable: String, opt: Boolean)
  case class Result(rNr: Int, col: Any)
  case class UnOp(operation: String, operand: Any)
  case class Fun(name: String, parameters: List[Any])
  case class BinOp(op: String, lop: Any, rop: Any)

  case class Obj(obj: Any, alias: String, join: Any, outerJoin: String)
  case class Col(col: Any, alias: String)
  case class Cols(distinct: Boolean, cols: List[Col])
  case class Grp(cols: List[Any], having: Any)
  //cols expression is tuple in the form - ([<nulls first>], <order col list>, [<nulls last>])
  case class Ord(cols: (Null, List[Any], Null), asc: Boolean)
  case class Query(tables: List[Obj], filter: Arr, cols: List[Col], distinct: Boolean,
    group: Grp, order: List[Ord], offset: Any, limit: Any)
  case class Arr(elements: List[Any])
  case class All()
  case class Null()

  case class Braces(expr: Any)

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
  def variable: Parser[Variable] = ((":" ~> (ident ~ opt("?"))) | "?") ^^ {
    case "?" => Variable("?", false)
    case (i: String) ~ o => Variable(i, o != None)
  }
  def result: Parser[Result] = (":" ~> "[0-9]+".r <~ "(") ~ ("[0-9]+".r | stringLiteral) <~ ")" ^^ {
    case r ~ c => Result(r.toInt, try { c.toInt } catch { case _: NumberFormatException => c })
  }
  def bracesExp: Parser[Braces] = "(" ~> expr <~ ")" ^^ (Braces(_))
  /* Important is that function parser is applied before query because of the longest token
     * matching, otherwise qualifiedIdent of query will match earlier.
     * Also important is that bracesExpr parser is applied before query otherwise it will always 
     * match down to query.
     * Also important is that array parser is applied after query parser because it matches join parser
     * of the query.
     * Also important is that variable parser is after query parser since ? mark matches variable */
  def operand: Parser[Any] = (TRUE | FALSE | NULL | ALL | decimalNr |
    stringLiteral | function | bracesExp | query | variable | result | array)
  def negation: Parser[UnOp] = "-" ~> operand ^^ (UnOp("-", _))
  def not: Parser[UnOp] = "!" ~> operand ^^ (UnOp("!", _))
  //is used within column clause to indicate separate query
  def sep: Parser[UnOp] = "|" ~> operand ^^ (UnOp("|", _))
  def function: Parser[Fun] = (ident <~ "(") ~ repsep(expr, ",") <~ ")" ^^ {
    case a ~ b => Fun(a, b)
  }
  def array: Parser[Arr] = "[" ~> repsep(expr, ",") <~ "]" ^^ (Arr(_))

  //query parsers
  def join: Parser[Any] = ("[" ~> repsep(expr, ",") <~ "]") | "/"
  def filter: Parser[Arr] = array
  def obj: Parser[Obj] = opt(join) ~ opt("?") ~ (qualifiedIdent | bracesExp) ~
    opt("?") ~ opt(excludeKeywordsIdent) ^^ {
      case a ~ Some(b) ~ c ~ Some(d) ~ e => error("Cannot be right and left join at the same time")
      case a ~ b ~ c ~ d ~ e => Obj(c, if (e == None) null else e.get, if (a == None) null
      else a.get, if (b != None) "r" else if (d != None) "l" else null)
    }
  def objs: Parser[List[Obj]] = rep1(obj)
  def column: Parser[Col] = expr ~ opt(stringLiteral | qualifiedIdent) ^^ {
    case e ~ a => Col(e, if (a == None) null else a.get match { case Ident(i) => i.mkString; case s => "\"" + s + "\"" })
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
      case t ~ f ~ c ~ g ~ o ~ l => Query(t, if (f == None) null else f.get,
        c.map(_.cols) orNull, c.map(_.distinct) getOrElse false,
        g.orNull, o.orNull, l.map(_._1) orNull, l.map(_._2) orNull)
    }

  //operation parsers
  def unaryExpr = negation | not | operand | sep
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

  private def binOp(p: ~[Any, List[~[String, Any]]]): Any = p match {
    case e ~ Nil => e
    case e ~ (l@((o ~ _) :: _)) => BinOp(o, e, binOp(l))
  }

  private def binOp(l: List[~[String, Any]]): Any = l match {
    case (_ ~ e) :: Nil => e
    case (_ ~ e) :: (l@((o ~ _) :: _)) => BinOp(o, e, binOp(l))
    case _ => error("Knipis")
  }

  def bindVariables(ex: String): List[String] = {
    var bindIdx = 0
    val vars = scala.collection.mutable.ListBuffer[String]()
    def bindVars(parsedExpr: Any): Any =
      parsedExpr match {
        case Variable("?", _) => bindIdx += 1; vars += bindIdx.toString
        case Variable(n, _) => vars += n
        case Fun(_, pars) => pars foreach (bindVars(_))
        case UnOp(_, operand) => bindVars(operand)
        case BinOp(_, lop, rop) => bindVars(lop); bindVars(rop)
        case Obj(t, _, j, _) => bindVars(j); bindVars(t)
        case Col(c, _) => bindVars(c)
        case Cols(_, cols) => cols foreach (bindVars(_))
        case Grp(cols, hv) => cols foreach (bindVars(_)); bindVars(hv)
        case Ord(cols, _) => cols._2 foreach (bindVars(_))
        case Query(objs, filter, cols, _, gr, ord, _, _) => {
          objs foreach (bindVars(_)); bindVars(filter)
          if (cols != null) cols foreach (bindVars(_))
          bindVars(gr);
          if (ord != null) ord foreach (bindVars(_))
        }
        case Arr(els) => els foreach (bindVars(_))
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