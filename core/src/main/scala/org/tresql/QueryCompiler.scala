package org.tresql

class QueryCompiler(override protected val metadata: Metadata, macros: MacroResources)
  extends QueryParser(macros) with compiling.Compiler {
  def compile(exp: String): parsing.Exp = compile(parseExp(exp))
}
