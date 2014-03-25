package com.allthingsmonitoring.hitachi

import spock.lang.*
import com.allthingsmonitoring.hitachi.HitachiPFM2Graphite


class HitachiPFM2GraphiteTest extends Specification{

    @Shared
    HitachiPFM2Graphite main
    def setupSpec() {
        main = new HitachiPFM2Graphite($/build\resources\test\config.groovy/$)
    }

    def "Test parsePMF - Header"() {
        when:
        Map data = main.parsePMF($/build\resources\test\pfm00000.txt/$)
        then:
        data.getClass() == java.util.LinkedHashMap

        data.sequenceNum == '1'
        data.intervalStart.toString() == 'Fri Mar 21 15:54:16 CET 2014'
        data.intervalEnd.toString() == 'Fri Mar 21 15:55:15 CET 2014'
        data.sn == '93050062'
    }
    def "Test parsePMF - Metrics"() {
        when:
        Map data = main.parsePMF($/build\resources\test\pfm00000.txt/$)
        then:
        data.getClass() == java.util.LinkedHashMap
        data.metrics.size() == 10

        data.metrics['Port'][0]['IO Rate(IOPS)'] == '395'
        data.metrics['Port'][0]['Trans Rate(MB/S)'] == '4'
        data.metrics['Processor'][0]['Usage(%)'] == '14'
        data.metrics['Management Area'][0]['Core'] == 'X'
    }
    def "Test normalizeMetricName"() {
        setup:
        ArrayList metrics = ['Trans Rate(MB/S)','Read Rate(IOPS)','Usage Rate(%)',
                             'XCOPY Time(microsec)','XCOPY Max Time(microsec.)','Write Job Delay CMD Count(1000ms-)',
                             'Sequential Read CMD Count','Random Write Trans Size(MB)','Average Tag','Cache(DRR) Bus Usage Rate(%)']
        when:
        ArrayList metricsNormalized = metrics.collect { main.normalizeMetricName(it) }
        then:
        metricsNormalized.size() == 10

        metricsNormalized[0] == 'TransRate_MBs'
        metricsNormalized[1] == 'ReadRate_IOPS'
        metricsNormalized[2] == 'UsageRate_PCT'
        metricsNormalized[3] == 'XCOPYTime_us'
        metricsNormalized[4] == 'XCOPYMaxTime_us'
        metricsNormalized[5] == 'WriteJobDelayCMD_1000ms_CNT'
        metricsNormalized[6] == 'SequentialReadCMD_CNT'
        metricsNormalized[7] == 'RandomWriteTransSize_MB'
        metricsNormalized[8] == 'AverageTag'
        metricsNormalized[9] == 'Cache-DRR-BusUsageRate_PCT'
    }
    def "Test buildMetrics : ignoreZeroValues = false"() {
        when:
        main.cfg.pfm.ignoreZeroValues = false
        Map data = main.parsePMF($/build\resources\test\pfm00000.txt/$)
        ArrayList metricList = main.buildMetrics(data)
        then:
        metricList.size() == 10886

        metricList[0] == 'SAN.93050062.Port.0.A.IORate_IOPS 395 1395413715\n'
        metricList[-1] == 'SAN.93050062.ManagementArea.1.Y.0.Access_CNT 0 1395413715\n'
    }
    def "Test buildMetrics : ignoreZeroValues = true"() {
        when:
        main.cfg.pfm.ignoreZeroValues = true
        Map data = main.parsePMF($/build\resources\test\pfm00000.txt/$)
        ArrayList metricList = main.buildMetrics(data)
        then:
        metricList.size() == 1982

        metricList[0] == 'SAN.93050062.Port.0.A.IORate_IOPS 395 1395413715\n'
        metricList[-1] == 'SAN.93050062.Backend.1.3.VerifyCMD_CNT 840 1395413715\n'
    }
    def "Test buildMetrics : Dump&Read Metrics"() {
        when:
        main.cfg.pfm.ignoreZeroValues = true
        Map data = main.parsePMF($/build\resources\test\pfm00988.txt/$)
        ArrayList metricList = main.buildMetrics(data)
        new File($/build\resources\test\pfm00988_dump.txt/$).withWriter('UTF-8') { w -> w << metricList.join('\n') }
        ArrayList metricDump = new File($/build\resources\test\pfm00988_dump.txt/$).readLines()
        then:
        metricDump.size() == 8123

        metricDump[-1] == 'SAN.93050089.Backend.1.3.VerifyCMD_CNT 840 1395675028'
    }
}
