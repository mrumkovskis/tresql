package org.tresql.parsing

import scala.util.parsing.combinator.syntactical.StandardTokenParsers

object QueryParsers extends StandardTokenParsers {
  
   lexical.reserved ++= Set("in", "null", "true", "false", "<=", ">=", "<", ">", "!=", "=", "~~",
       "!~~", "~", "!~", "in", "!in", "++", "+", "-", "&&", "||", "*", "/", "!", "?")
   lexical.delimiters ++= Set("(", ")", "[", "]", "{", "}", "#", "@", "^", ",", ".", ":", ":#")

}