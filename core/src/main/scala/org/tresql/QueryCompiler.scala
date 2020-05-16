package org.tresql

class QueryCompiler(res: Resources) extends QueryParser(res) with compiling.Compiler {
  def compile(exp: String): parsing.Exp = compile(parseExp(exp))
}
