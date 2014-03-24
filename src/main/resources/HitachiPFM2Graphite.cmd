@echo off

set APP_HOME=E:\HitachiPFM2Graphite
set JAVA_OPTS=-server -Xmx800M -Xms350M -XX:MaxPermSize=80m -XX:+CMSClassUnloadingEnabled -XX:+UseConcMarkSweepGC

java -Dapp.env=PROD -jar %APP_HOME%\HitachiPFM2Graphite-1.0.jar >> %APP_HOME%\logs\HitachiPFM2Graphite_exec.log 2>&1

