package org.tresql

class QueryCompiler(override protected val metadata: Metadata,
                    override protected val extraMetadata: Map[String, Metadata],
                    macros: MacroResources)
  extends QueryParser(macros, null) with compiling.Compiler {
  def compile(exp: String): parsing.Exp = compile(parseExp(exp))
}
