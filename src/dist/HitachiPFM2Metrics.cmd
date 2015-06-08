@echo off

set APP_HOME=C:\HitachiPFM2Metrics
set JAVA_OPTS=-server -Xmx512M -Xms128M -XX:MaxPermSize=80m -XX:+AggressiveOpts -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:+CMSClassUnloadingEnabled -XX:+UseCMSInitiatingOccupancyOnly -XX:+UseCompressedOops

java -Dapp.env=PROD -jar %APP_HOME%\HitachiPFM2Metrics-1.2.jar >> %APP_HOME%\logs\HitachiPFM2Metrics_exec.log 2>&1

