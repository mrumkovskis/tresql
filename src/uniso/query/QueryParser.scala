package uniso.query

import scala.util.parsing.combinator.JavaTokenParsers
import scala.util.parsing.input.Reader

object QueryParser extends JavaTokenParsers {

    case class Ident(ident: List[String])
    case class Variable(variable: String, opt: Boolean)
    case class Result(rNr: Int, col: Any)
    case class UnOp(operation: String, operand: Any)
    case class Fun(name: String, parameters: List[Any])
    case class BinOp(op: String, lop: Any, rop: Any)

    case class Obj(obj: Any, alias: String, join: Any, outerJoin: String)
    case class Col(col: Any, alias: String)
    case class Grp(cols: List[Any], having: Any)
    case class Ord(cols: List[Any], asc: Boolean)
    case class Query(tables: List[Obj], filter: Arr, cols: List[Col], group: Grp, order: List[Ord])
    case class Arr(elements: List[Any])
    case class All()
    case class Null()
    
    case class Braces(expr: Any)

    /*TODO allow string literal to be enclosed into single quotes*/
    /* allow double quotes in string literal */
    override def stringLiteral: Parser[String] =
      ("\""+"""([^"\p{Cntrl}\\]|\\[\\/bfnrt"]|\\u[a-fA-F0-9]{4})*"""+"\"").r ^^ 
          (s => s.substring(1, s.length - 1).replace("\\\"", "\""))
    def comment: Parser[String] = """/\*(.|[\r\n])*?\*/""".r
    def decimalNr: Parser[BigDecimal] = decimalNumber ^^ (BigDecimal(_))
    def TRUE: Parser[Boolean] = "true" ^^^ true
    def FALSE: Parser[Boolean] = "false" ^^^ false 
    def NULL = "null" ^^^ Null()
    def ALL: Parser[All] = "*" ^^^ All()
    
    def qualifiedIdent: Parser[Ident] = rep1sep(ident, ".") ^^ (Ident(_))
    def variable: Parser[Variable] = ((":" ~> (ident ~ opt("?"))) | "?") ^^ {
        case "?" => Variable("?", false)
        case (i:String) ~ o => Variable(i, o != None)
    }
    def result: Parser[Result] = (":" ~> "[0-9]+".r <~ "(") ~ ("[0-9]+".r | stringLiteral) <~ ")" ^^ {
      case r ~ c => Result(r.toInt, try { c.toInt } catch {case _:NumberFormatException => c})
    }
    def bracesExp: Parser[Braces] = "(" ~> expr <~ ")" ^^ (Braces(_))
    /* Imporant is that function parser is applied before query because of the longest token
     * matching, otherwise qualifiedIdent of query will match earlier.
     * Also important is that bracesExpr parser is applied before query otherwise it will allways match down to query.
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
      opt("?") ~ opt(ident) ^^ {
        case a ~ Some(b) ~ c ~ Some(d) ~ e => error("Cannot be right and left join at the same time")        
        case a ~ b ~ c ~ d ~ e => Obj(c, if (e == None) null else e.get, if (a == None) null
        else a.get, if (b != None) "r" else if (d != None) "l" else null)
    }
    def objs: Parser[List[Obj]] = rep1(obj)
    def column: Parser[Col] = expr ~ opt(stringLiteral) ^^ { case e ~ a => Col(e,
            if(a == None) null else a.get) }
    def columns: Parser[List[Col]] = "{" ~> rep1sep(column, ",") <~ "}"
    def group: Parser[Grp] = (("@" ~ "(") ~> rep1sep(expr, ",") <~ ")") ~ 
        opt(("^" ~ "(") ~> expr <~ ")") ^^ {case g ~ h => Grp(g, if (h == None) null else h.get)}
    def orderAsc: Parser[Ord] = ("#" ~ "(") ~> rep1sep(expr, ",") <~ ")" ^^ (Ord(_, true))
    def orderDesc: Parser[Ord] = ("~#" ~ "(") ~> rep1sep(expr, ",") <~ ")" ^^ (Ord(_, false))
    def order: Parser[List[Ord]] = rep1(orderAsc | orderDesc)
    def query: Parser[Any] = objs ~ opt(filter) ~ opt(columns) ~ opt(group) ~ opt(order) ^^ {
      case (t :: Nil) ~ None ~ None ~ None ~ None => t
      case t ~ f ~ c ~ g ~ o => Query(t, if (f == None) null else f.get,
        if(c == None) null else c.get, if(g == None) null else g.get, if(o == None) null else o.get)
    }
        
    //operation parsers
    def unaryExpr = negation | not | operand | sep
    def mulDiv: Parser[Any] = unaryExpr ~ rep("*" ~ unaryExpr | "/" ~ unaryExpr) ^^ (p => binOp(p))
    def plusMinus: Parser[Any] = mulDiv ~ rep("+" ~ mulDiv | "-" ~ mulDiv) ^^ (p => binOp(p))
    def comp: Parser[Any] = plusMinus ~ 
        opt(("<=" | ">=" | "<" | ">" | "!=" | "=" | "~") ~ plusMinus) ^^ {
        case l ~ None => l
        case l ~ Some(o ~ r) => BinOp(o, l, r)
    }
    def logical: Parser[Any] = comp ~ rep("&" ~ comp | "|" ~ comp) ^^ (p => binOp(p))

    //expression
    def expr: Parser[Any] = opt(comment) ~> logical <~ opt(comment)
    
    
    def binOp(p: ~[Any, List[~[String, Any]]]): Any = p match {
        case e ~ Nil => e
        case e ~ (l@((o ~ _) :: _)) => BinOp(o, e, binOp(l))
    }
    
    def binOp(l: List[~[String, Any]]): Any = l match {
        case (_ ~ e) :: Nil => e
        case (_ ~ e) :: (l@((o ~ _) :: _)) => BinOp(o, e, binOp(l))
        case _ => error("Knipis")
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