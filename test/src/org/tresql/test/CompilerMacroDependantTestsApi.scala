package org.tresql.test

import org.tresql.Resources

trait CompilerMacroDependantTestsApi {
  def api(implicit resources: Resources): Unit
  def ort(implicit resources: Resources): Unit
  def compilerMacro(implicit resources: Resources): Unit
}
