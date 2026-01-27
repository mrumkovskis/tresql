package org.tresql

class QueryCompiler(override val metadata: Metadata,
                    override val extraMetadata: Map[String, Metadata],
                    macros: MacroResources)
  extends QueryParser(macros, null) with compiling.Compiler {
  def compile(exp: String): ast.Exp = compile(parseExp(exp))
}
