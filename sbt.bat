set SCRIPT_DIR=%~dp0

set JAVA_OPTS=
if exist "%SCRIPT_DIR%\setenv.bat" call "%SCRIPT_DIR%\setenv.bat" %1

java %JAVA_OPTS% -XX:+CMSClassUnloadingEnabled -XX:MaxPermSize=256m -Xmx512M -Xss2M -jar "%SCRIPT_DIR%\sbt-launcher.jar" %*
