package org.tresql.test

import org.tresql.Resources

trait PGCompilerMacroDependantTestsApi {
  def api(implicit resources: Resources): Unit
  def ort(implicit resources: Resources): Unit
  def compilerMacro(implicit resources: Resources): Unit
}
