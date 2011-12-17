set SCRIPT_DIR=%~dp0
if exist "%SCRIPT_DIR%\setenv.bat" call "%SCRIPT_DIR%\setenv.bat" %1
java -Xmx512M -jar "%SCRIPT_DIR%sbt-launch.jar" %*