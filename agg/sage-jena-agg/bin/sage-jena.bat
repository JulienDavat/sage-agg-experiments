@if "%DEBUG%" == "" @echo off
@rem ##########################################################################
@rem
@rem  sage-jena startup script for Windows
@rem
@rem ##########################################################################

@rem Set local scope for the variables with windows NT shell
if "%OS%"=="Windows_NT" setlocal

set DIRNAME=%~dp0
if "%DIRNAME%" == "" set DIRNAME=.
set APP_BASE_NAME=%~n0
set APP_HOME=%DIRNAME%..

@rem Add default JVM options here. You can also use JAVA_OPTS and SAGE_JENA_OPTS to pass JVM options to this script.
set DEFAULT_JVM_OPTS=

@rem Find java.exe
if defined JAVA_HOME goto findJavaFromJavaHome

set JAVA_EXE=java.exe
%JAVA_EXE% -version >NUL 2>&1
if "%ERRORLEVEL%" == "0" goto init

echo.
echo ERROR: JAVA_HOME is not set and no 'java' command could be found in your PATH.
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:findJavaFromJavaHome
set JAVA_HOME=%JAVA_HOME:"=%
set JAVA_EXE=%JAVA_HOME%/bin/java.exe

if exist "%JAVA_EXE%" goto init

echo.
echo ERROR: JAVA_HOME is set to an invalid directory: %JAVA_HOME%
echo.
echo Please set the JAVA_HOME variable in your environment to match the
echo location of your Java installation.

goto fail

:init
@rem Get command-line arguments, handling Windows variants

if not "%OS%" == "Windows_NT" goto win9xME_args

:win9xME_args
@rem Slurp the command line arguments.
set CMD_LINE_ARGS=
set _SKIP=2

:win9xME_args_slurp
if "x%~1" == "x" goto execute

set CMD_LINE_ARGS=%*

:execute
@rem Setup the command line

set CLASSPATH=%APP_HOME%\lib\sage-jena-1.1.jar;%APP_HOME%\lib\google-http-client-jackson2-1.23.0.jar;%APP_HOME%\lib\google-http-client-1.23.0.jar;%APP_HOME%\lib\guava-21.0.jar;%APP_HOME%\lib\jena-tdb-3.7.0.jar;%APP_HOME%\lib\jena-rdfconnection-3.7.0.jar;%APP_HOME%\lib\jena-tdb2-3.7.0.jar;%APP_HOME%\lib\jena-dboe-trans-data-3.7.0.jar;%APP_HOME%\lib\jena-dboe-transaction-3.7.0.jar;%APP_HOME%\lib\jena-dboe-index-3.7.0.jar;%APP_HOME%\lib\jena-dboe-base-3.7.0.jar;%APP_HOME%\lib\jena-arq-3.7.0.jar;%APP_HOME%\lib\jsonld-java-0.11.1.jar;%APP_HOME%\lib\jena-core-3.7.0.jar;%APP_HOME%\lib\jena-base-3.7.0.jar;%APP_HOME%\lib\commons-io-2.6.jar;%APP_HOME%\lib\picocli-3.9.5.jar;%APP_HOME%\lib\slf4j-log4j12-1.7.5.jar;%APP_HOME%\lib\jena-shaded-guava-3.7.0.jar;%APP_HOME%\lib\libthrift-0.10.0.jar;%APP_HOME%\lib\jcl-over-slf4j-1.7.25.jar;%APP_HOME%\lib\jena-iri-3.7.0.jar;%APP_HOME%\lib\slf4j-api-1.7.25.jar;%APP_HOME%\lib\jackson-databind-2.9.0.jar;%APP_HOME%\lib\jackson-core-2.9.0.jar;%APP_HOME%\lib\jsr305-1.3.9.jar;%APP_HOME%\lib\httpclient-cache-4.5.3.jar;%APP_HOME%\lib\httpclient-4.5.3.jar;%APP_HOME%\lib\log4j-1.2.17.jar;%APP_HOME%\lib\httpcore-4.4.6.jar;%APP_HOME%\lib\commons-logging-1.2.jar;%APP_HOME%\lib\commons-codec-1.11.jar;%APP_HOME%\lib\commons-lang3-3.4.jar;%APP_HOME%\lib\xercesImpl-2.11.0.jar;%APP_HOME%\lib\commons-cli-1.4.jar;%APP_HOME%\lib\xml-apis-1.4.01.jar;%APP_HOME%\lib\commons-csv-1.5.jar;%APP_HOME%\lib\collection-0.7.jar;%APP_HOME%\lib\jackson-annotations-2.9.0.jar

@rem Execute sage-jena
"%JAVA_EXE%" %DEFAULT_JVM_OPTS% %JAVA_OPTS% %SAGE_JENA_OPTS%  -classpath "%CLASSPATH%" org.gdd.sage.cli.CLI %CMD_LINE_ARGS%

:end
@rem End local scope for the variables with windows NT shell
if "%ERRORLEVEL%"=="0" goto mainEnd

:fail
rem Set variable SAGE_JENA_EXIT_CONSOLE if you need the _script_ return code instead of
rem the _cmd.exe /c_ return code!
if  not "" == "%SAGE_JENA_EXIT_CONSOLE%" exit 1
exit /b 1

:mainEnd
if "%OS%"=="Windows_NT" endlocal

:omega
