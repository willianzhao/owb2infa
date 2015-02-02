cd /d "%ORACLE_HOME%\owb11gr2\owb\bin\win32"
call changecodepage
call setowbenv.bat

set CLASSPATH=.;%ORACLE_HOME%\owb11gr2\owb\bin\win32;%ORACLE_HOME%\owb11gr2\jdbc\lib;%ORACLE_HOME%\owb11gr2\owb\bin\win32;%ORACLE_HOME%\owb11gr2\owb\bin\win32\resource;%CLASSPATH%
set OWB2INFA_JAR=owb2infa.jar

java -Xms1024M -Xmx2048M -XX:MaxPermSize=512M -Dfile.encoding=UTF8 -DORACLE_HOME="%ORACLE_HOME%\owb11gr2" -DOWBCC_HOME="%ORACLE_HOME%\owb11gr2" -DTCLLIBPATH="%TCLLIBPATH%" -Dide.conf="%ORACLE_HOME%\owb11gr2\owb\bin\owb.conf" -cp %CLASSPATH% -jar %ORACLE_HOME%\owb11gr2\owb\bin\win32\%OWB2INFA_JAR% 12

cd /d "%ORACLE_HOME%\owb11gr2\owb\bin\win32"