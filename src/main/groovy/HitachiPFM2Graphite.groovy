package com.allthingsmonitoring.hitachi

import org.slf4j.*
import groovy.util.logging.Slf4j
import ch.qos.logback.classic.*
import static ch.qos.logback.classic.Level.*
import org.codehaus.groovy.runtime.StackTraceUtils
import groovyx.gpars.GParsPool
import groovyx.gpars.util.PoolUtils

import java.util.regex.Pattern
import java.util.regex.Matcher
import java.util.zip.*
import java.util.jar.Manifest
import java.util.jar.Attributes

import groovy.time.*
import java.text.SimpleDateFormat

import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.Files
import static java.nio.file.StandardCopyOption.*



@Slf4j
class HitachiPFM2Graphite {

  ConfigObject cfg
  Map parseCfg = [:]


  /**
   * Constructor
   */
  HitachiPFM2Graphite(String cfgFile='config.groovy') {
    cfg = readConfigFile(cfgFile)
    Attributes manifest = getManifestInfo()
    loadParserCfg()
    log.info "Initialization ${this.class.name}: Version: ${manifest?.getValue('Specification-Version')} / Built-Date: ${manifest?.getValue('Built-Date')}"
  }


  /**
   * Load configuration settings
   *
   * @return ConfigObject with the configuration elements
   */
  ConfigObject readConfigFile(String cfgFile) {
    try {
      ConfigObject cfg = new ConfigSlurper().parse(new File(cfgFile).toURL())
      if (cfg) {
        log.trace "The configuration files: ${cfgFile} was read correctly"
        // Default config
        cfg.nnm.collect_metrics = cfg.nnm.collect_metrics ?: ['All']
        return cfg
      } else {
        log.error "Verify the content of the configuration file: ${cfgFile}"
        throw new RuntimeException("Verify the content of the configuration file: ${cfgFile}")
      }
    } catch(FileNotFoundException e) {
      log.error "The configuration file: ${cfgFile} was not found"
      throw new RuntimeException("The configuration file: ${cfgFile} was not found")
    } catch(Exception e) {
      StackTraceUtils.deepSanitize(e)
      log.error "Configuration file exception: ${getStackTrace(e)}"
      throw new RuntimeException("Configuration file exception:\n${getStackTrace(e)}")
    }
  }


  /**
   * Retrieves the Manifest Info from the JAR file
   *
   * @return JAR MainAttributes
   */
  Attributes getManifestInfo() {
    Class clazz = this.getClass()
    String className = clazz.getSimpleName() + ".class"
    String classPath = clazz.getResource(className).toString()
    // Class not from JAR
    if (!classPath.startsWith("jar")) { return null }

    String manifestPath = classPath.substring(0, classPath.lastIndexOf('!') + 1) + "/META-INF/MANIFEST.MF"
    Manifest manifest
    try {
      manifest = new Manifest(new URL(manifestPath).openStream())
    } catch(Exception e) {
      StackTraceUtils.deepSanitize(e)
      log.warn "Manifest: ${getStackTrace(e)}"
    }

    return manifest.getMainAttributes()
  }

  // Gets the StackTrace and returns a string
  String getStackTrace(Throwable t) {
    StringWriter sw = new StringWriter()
    PrintWriter pw = new PrintWriter(sw, true)
    t.printStackTrace(pw)
    pw.flush()
    sw.flush()
    return sw.toString()
  }



  /**
   * Finds PFM files
   *
   * @return ArrayList of all the found PFM file
   */
  ArrayList findPMFFiles() {
    ArrayList pmfFiles = []

    cfg?.pfm?.metrics?.each { String folder ->
      if (!new File(folder).exists()){ log.error "The PMF Metrics folder does not exist: ${folder}"; return }

      // Retrieves all the Metric files and sorts them by lastModified
      new File(folder).listFiles().sort{ a,b -> a.lastModified() <=> b.lastModified() }.each {
        if (it.isDirectory()) { return }
        log.trace "PMF Performance: ${new Date(it.lastModified())} - ${it.lastModified()} - ${it.name}"
        pmfFiles << it.canonicalPath
      }
    }

    if (pmfFiles) {
      log.info "Found ${pmfFiles.size()} PMF Metric files"
    } else {
      log.warn "No PMF Metric files where found"
    }

    return pmfFiles
  }



  /**
   * Load parsing configuration
   *
   */
  void loadParserCfg(){
    this.parseCfg['Port'] = [1:[columns: ['CTL','Port','IO Rate(IOPS)','Read Rate(IOPS)','Write Rate(IOPS)','Read Hit(%)','Write Hit(%)','Trans Rate(MB/S)','Read Trans Rate(MB/S)', 'Write Trans Rate(MB/S)']],
                             2:[columns: ['CTL','Port','Read CMD Count','Write CMD Count','Read CMD Hit Count','Write CMD Hit Count','Read Trans Size(MB)','Write Trans Size(MB)']],
                             3:[columns: ['CTL','Port','CTL CMD IO Rate(IOPS)','CTL CMD Trans Rate(KB/S)','CTL CMD Count','CTL CMD Trans Size(KB)','CTL CMD Time(microsec)','CTL CMD Max Time(microsec)']],
                             4:[columns: ['CTL','Port','Data CMD IO Rate(IOPS)','Data CMD Trans Rate(MB/S)','Data CMD Count','Data CMD Trans Size(MB)','Data CMD Time(microsec)','Data CMD Max Time(microsec)']],
                             5:[columns: ['CTL','Port','Timeout Error Count']],
                             6:[columns: ['CTL','Port','Random IO Rate(IOPS)','Random Read Rate(IOPS)','Random Write Rate(IOPS)','Random Trans Rate(MB/S)','Random Read Trans Rate(MB/S)','Random Write Trans Rate(MB/S)']],
                             7:[columns: ['CTL','Port','Random Read CMD Count','Random Write CMD Count','Random Read Trans Size(MB)','Random Write Trans Size(MB)']],
                             8:[columns: ['CTL','Port','Sequential IO Rate(IOPS)','Sequential Read Rate(IOPS)','Sequential Write Rate(IOPS)','Sequential Trans Rate(MB/S)','Sequential Read Trans Rate(MB/S)','Sequential Write Trans Rate(MB/S)']],
                             9:[columns: ['CTL','Port','Sequential Read CMD Count','Sequential Write CMD Count','Sequential Read Trans Size(MB)','Sequential Write Trans Size(MB)']],
                             10:[columns: ['CTL','Port','XCOPY Rate(IOPS)','XCOPY Read Rate(IOPS)','XCOPY Write Rate(IOPS)','XCOPY Read Trans Rate(MB/S)','XCOPY Write Trans Rate(MB/S)']],
                             11:[columns: ['CTL','Port','XCOPY Time(microsec)','XCOPY Max Time(microsec)']]
                            ]

    this.parseCfg['RG'] = [1:[columns: ['CTL','RG','IO Rate(IOPS)','Read Rate(IOPS)','Write Rate(IOPS)','Read Hit(%)','Write Hit(%)','Trans Rate(MB/S)','Read Trans Rate(MB/S)','Write Trans Rate(MB/S)']],
                           2:[columns: ['CTL','RG','Read CMD Count','Write CMD Count','Read CMD Hit Count','Write CMD Hit Count','Read Trans Size(MB)','Write Trans Size(MB)']],
                           3:[columns: ['CTL','RG','Random IO Rate(IOPS)','Random Read Rate(IOPS)','Random Write Rate(IOPS)','Random Trans Rate(MB/S)','Random Read Trans Rate(MB/S)','Random Write Trans Rate(MB/S)']],
                           4:[columns: ['CTL','RG','Random Read CMD Count','Random Write CMD Count','Random Read Trans Size(MB)','Random Write Trans Size(MB)']],
                           5:[columns: ['CTL','RG','Sequential IO Rate(IOPS)','Sequential Read Rate(IOPS)','Sequential Write Rate(IOPS)','Sequential Trans Rate(MB/S)','Sequential Read Trans Rate(MB/S)','Sequential Write Trans Rate(MB/S)']],
                           6:[columns: ['CTL','RG','Sequential Read CMD Count','Sequential Write CMD Count','Sequential Read Trans Size(MB)','Sequential Write Trans Size(MB)']],
                           7:[columns: ['CTL','RG','XCOPY Rate(IOPS)','XCOPY Read Rate(IOPS)','XCOPY Write Rate(IOPS)','XCOPY Read Trans Rate(MB/S)','XCOPY Write Trans Rate(MB/S)']],
                           8:[columns: ['CTL','RG','XCOPY Time(microsec)','XCOPY Max Time(microsec)']]
                          ]

    this.parseCfg['DP Pool'] = [1:[columns: ['CTL','DP Pool','IO Rate(IOPS)','Read Rate(IOPS)','Write Rate(IOPS)','Read Hit(%)','Write Hit(%)','Trans Rate(MB/S)','Read Trans Rate(MB/S)','Write Trans Rate(MB/S)']],
                                2:[columns: ['CTL','DP Pool','Read CMD Count','Write CMD Count','Read CMD Hit Count','Write CMD Hit Count','Read Trans Size(MB)','Write Trans Size(MB)']],
                                3:[columns: ['CTL','DP Pool','XCOPY Rate(IOPS)','XCOPY Read Rate(IOPS)','XCOPY Write Rate(IOPS)','XCOPY Read Trans Rate(MB/S)','XCOPY Write Trans Rate(MB/S)']],
                                4:[columns: ['CTL','DP Pool','XCOPY Time(microsec)','XCOPY Max Time(microsec)']]
                               ]


    this.parseCfg['LU'] = [1:[columns: ['CTL','LU','IO Rate(IOPS)','Read Rate(IOPS)','Write Rate(IOPS)','Read Hit(%)','Write Hit(%)','Trans Rate(MB/S)','Read Trans Rate(MB/S)','Write Trans Rate(MB/S)']],
                           2:[columns: ['CTL','LU','Read CMD Count','Write CMD Count','Read CMD Hit Count','Write CMD Hit Count','Read Trans Size(MB)','Write Trans Size(MB)']],
                           3:[columns: ['CTL','LU','Read CMD Hit Count2','Read CMD Hit Time(microsec)','Read CMD Hit Max Time(microsec)']],
                           4:[columns: ['CTL','LU','Write CMD Hit Count2','Write CMD Hit Time(microsec)','Write CMD Hit Max Time(microsec)']],
                           5:[columns: ['CTL','LU','Read CMD Miss Count','Read CMD Miss Time(microsec)','Read CMD Miss Max Time(microsec)']],
                           6:[columns: ['CTL','LU','Write CMD Miss Count','Write CMD Miss Time(microsec)','Write CMD Miss Max Time(microsec)']],
                           7:[columns: ['CTL','LU','Read CMD Job Count','Read CMD Job Time(microsec)','Read CMD Job Max Time(microsec)']],
                           8:[columns: ['CTL','LU','Write CMD Job Count','Write CMD Job Time(microsec)','Write CMD Job Max Time(microsec)']],
                           9:[columns: ['CTL','LU','Read Hit Delay CMD Count(<300ms)','Read Hit Delay CMD Count(300-499ms)','Read Hit Delay CMD Count(500-999ms)','Read Hit Delay CMD Count(1000ms-)']],
                           10:[columns: ['CTL','LU','Write Hit Delay CMD Count(<300ms)','Write Hit Delay CMD Count(300-499ms)','Write Hit Delay CMD Count(500-999ms)','Write Hit Delay CMD Count(1000ms-)']],
                           11:[columns: ['CTL','LU','Read Miss Delay CMD Count(<300ms)','Read Miss Delay CMD Count(300-499ms)','Read Miss Delay CMD Count(500-999ms)','Read Miss Delay CMD Count(1000ms-)']],
                           12:[columns: ['CTL','LU','Write Miss Delay CMD Count(<300ms)','Write Miss Delay CMD Count(300-499ms)','Write Miss Delay CMD Count(500-999ms)','Write Miss Delay CMD Count(1000ms-)']],
                           13:[columns: ['CTL','LU','Read Job Delay CMD Count(<300ms)','Read Job Delay CMD Count(300-499ms)','Read Job Delay CMD Count(500-999ms)','Read Job Delay CMD Count(1000ms-)']],
                           14:[columns: ['CTL','LU','Write Job Delay CMD Count(<300ms)','Write Job Delay CMD Count(300-499ms)','Write Job Delay CMD Count(500-999ms)','Write Job Delay CMD Count(1000ms-)']],
                           15:[columns: ['CTL','LU','Tag Count','Average Tag Count']],
                           16:[columns: ['CTL','LU','Data CMD IO Rate(IOPS)','Data CMD Trans Rate(MB/S)','Data CMD Count','Data CMD Trans Size(MB)','Data CMD Time(microsec)','Data CMD Max Time(microsec)']],
                           17:[columns: ['CTL','LU','Random IO Rate(IOPS)','Random Read Rate(IOPS)','Random Write Rate(IOPS)','Random Trans Rate(MB/S)','Random Read Trans Rate(MB/S)','Random Write Trans Rate(MB/S)']],
                           18:[columns: ['CTL','LU','Random Read CMD Count','Random Write CMD Count','Random Read Trans Size(MB)','Random Write Trans Size(MB)']],
                           19:[columns: ['CTL','LU','Sequential IO Rate(IOPS)','Sequential Read Rate(IOPS)','Sequential Write Rate(IOPS)','Sequential Trans Rate(MB/S)','Sequential Read Trans Rate(MB/S)','Sequential Write Trans Rate(MB/S)']],
                           20:[columns: ['CTL','LU','Sequential Read CMD Count','Sequential Write CMD Count','Sequential Read Trans Size(MB)','Sequential Write Trans Size(MB)']],
                           21:[columns: ['CTL','LU','XCOPY Rate(IOPS)','XCOPY Read Rate(IOPS)','XCOPY Write Rate(IOPS)','XCOPY Read Trans Rate(MB/S)','XCOPY Write Trans Rate(MB/S)']],
                           22:[columns: ['CTL','LU','XCOPY Time(microsec)','XCOPY Max Time(microsec)']],
                           23:[columns: ['CTL','LU','Total Tag Count','Read Tag Count','Write Tag Count','Total Average Tag Count','Read Average Tag Count','Write Average Tag Count']]
                          ]


    this.parseCfg['Cache'] = [1:[columns: ['CTL','Write Pending Rate(%)']],
                              2:[columns: ['CTL','Clean Queue Usage Rate(%)','Middle Queue Usage Rate(%)','Physical Queue Usage Rate(%)','Total Queue Usage Rate(%)']],
                              3:[columns: ['CTL','Partition','Write Pending Rate(%)']],
                              4:[columns: ['CTL','Partition','Clean Queue Usage Rate(%)','Middle Queue Usage Rate(%)','Physical Queue Usage Rate(%)']]
                             ]

    this.parseCfg['Processor'] = [1:[columns: ['CTL','Core','Usage(%)']],
                                  2:[columns: ['CTL','Host-Cache Bus Usage Rate(%)','Drive-Cache Bus Usage Rate(%)','Processor-Cache Bus Usage Rate(%)']],
                                  3:[columns: ['CTL','Cache(DRR) Bus Usage Rate(%)','Dual Bus Usage Rate(%)','Total Bus Usage Rate(%)']]
                                 ]

    this.parseCfg['Drive'] = [1:[columns: ['CTL','Unit','HDU','IO Rate(IOPS)','Read Rate(IOPS)','Write Rate(IOPS)','Trans Rate(MB/S)','Read Trans Rate(MB/S)','Write Trans Rate(MB/S)','Online Verify Rate(IOPS)']],
                              2:[columns: ['CTL','Unit','HDU','Read CMD Count','Write CMD Count','Read Trans Size','Write Trans Size Online','Verify CMD Count']]
                             ]

    this.parseCfg['Drive Operate'] = [1:[columns: ['CTL','Unit','HDU','Operating Rate(%)','Tag Count','Unload Time(min)','Average Tag Count']]]

    this.parseCfg['Backend'] = [1:[columns: ['CTL','Path','IO Rate(IOPS)','Read Rate(IOPS)','Write Rate(IOPS)','Trans Rate(MB/S)','Read Trans Rate(MB/S)','Write Trans Rate(MB/S)','Online Verify Rate(IOPS)']],
                                2:[columns: ['CTL','Path','Read CMD Count','Write CMD Count','Read Trans Size','Write Trans Size Online','Verify CMD Count']]
                               ]

    this.parseCfg['Management Area'] = [1:[columns: ['CTL','Core','DP Pool','Cache Hit Rate(%)','Access Count']]]

  }


  /**
   * Build the Metrics from the parsed PFM file
   *
   * @param file String path to the PFM file
   * @return Map with the parsed PFM Metrics
   */
  Map parsePMF(String file) {
    Date timeStart = new Date()
    String sequenceNum
    Pattern intervalPat = ~/(?i)(.*)\s-\s(.*)\s-\sSN:(.*)/
    Date intervalStart
    Date intervalEnd
    String sn

    Pattern sectionPat = ~/(?i)^----\s(.*)\sInformation.*----/
    String sectionName
    int sectionIdx = 0

    Map data = [:]
    MapWithDefault metrics = [:].withDefault { [] }

    try {
      new File(file).eachLine { String line ->
        // Seq
        if (line =~ /^No\./) {
           sequenceNum = line - 'No.'
           log.trace "Seq# ${sequenceNum}"
           return
        }

        if (line =~ /(?i).*skipped.*/) {
           log.trace "Ignoring file: skipped"
           throw new RuntimeException("Ignoring file")
        }

        // Interval
        // 2014/03/21 15:54:16 - 2014/03/21 15:55:15 - SN:93050062
        Matcher m = intervalPat.matcher(line)
        if (m.find()) {
          log.trace "Interval: S:${m?.group(1)?.trim()} -> E:${m?.group(2)?.trim()} / SN:${m?.group(3)?.trim()}"
          String sDate = m?.group(1)?.trim()
          String eDate = m?.group(2)?.trim()
          if (sDate && eDate) {
            intervalStart = new Date().parse('yyyy/MM/dd HH:mm:ss', sDate)
            intervalEnd = new Date().parse('yyyy/MM/dd HH:mm:ss', eDate)
          } else {
            log.error "Intervals not found: ${intervalStart} / ${sDate} nor ${intervalEnd} / ${eDate}"
          }
          sn = m?.group(3)?.trim()
          return
        }

        // Section
        // Find Section and reset Section index
        m = sectionPat.matcher(line)
        if (m.find()) {
          log.trace "Section: ${m?.group(1)?.trim()}"
          sectionName = m?.group(1)?.trim()
          sectionIdx = 0
          return
        }

        // Header
        if(line =~ /^CTL/) {
          log.trace "Header: ${sectionName}"
          sectionIdx++
          return
        }

        // Merge Headers with there corresponding Metrics
        if(parseCfg.containsKey(sectionName) && parseCfg[sectionName].containsKey(sectionIdx)) {
          metrics[sectionName] << [parseCfg[sectionName][sectionIdx]?.columns, line.tokenize()].transpose().inject([:]) { a, b -> a[b[0]] = b[1]; a }
        } else {
          log.error "No parsing cfg found for the section: ${sectionName} -> ${sectionIdx}"
        }
      }
    } catch(Exception e) {
      StackTraceUtils.deepSanitize(e)
      log.warn "Ignoring file: ${file}"
      return data
    }

    data['sequenceNum'] = sequenceNum
    data['intervalStart'] = intervalStart
    data['intervalEnd'] = intervalEnd
    data['sn'] = sn
    data['metrics'] = metrics

    Date timeEnd = new Date()
    log.info "Finished parsing PFM file Seq#: ${sequenceNum} in ${TimeCategory.minus(timeEnd,timeStart)}"

    return data
  }

  /**
   * Build the Metrics from the parsed PFM file
   *
   * @param data Map containing the parsed PFM file
   * @return ArrayList of all the metrics ready to be send to Graphite
   */
  ArrayList buildMetrics(Map data) {
    if (!data) { return [] }

    Date timeStart = new Date()
    ArrayList metricList = []
    int mtimes = (data['intervalEnd'].time.toLong()/1000).toInteger()

    log.info "Start Bulding Metrics for SN: ${data['sn']} / Seq#: ${data['sequenceNum']} / Periode: ${TimeCategory.minus(data['intervalEnd'],data['intervalStart'])} / Interval: ${data['intervalEnd']}"
    data['metrics']?.each { String sectionName, ArrayList metrics ->
      log.debug "Section: ${sectionName} [${metrics?.size()}]"


      switch(sectionName) {
        case 'Port':
          metrics.each { metric ->
            metric.each { field ->
              if (field.key =~ /CTL|Port/) { return }
              if (cfg?.pfm?.ignoreZeroValues && !field?.value?.toInteger()) { return }
              String mname = normalizeMetricName(field.key)
              metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.${metric.Port}.${mname} ${field.value} ${mtimes}\n"
            }
          }
        break
        case 'RG':
          metrics.each { metric ->
            metric.each { field ->
              if (field.key =~ /CTL|RG/) { return }
              if (cfg?.pfm?.ignoreZeroValues && !field?.value?.toInteger()) { return }
              String mname = normalizeMetricName(field.key)
              metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.${metric.RG}.${mname} ${field.value} ${mtimes}\n"
            }
          }
        break
        case 'DP Pool':
          metrics.each { metric ->
            metric.each { field ->
              if (field.key =~ /CTL|DP Pool/) { return }
              if (cfg?.pfm?.ignoreZeroValues && !field?.value?.toInteger()) { return }
              String mname = normalizeMetricName(field.key)
              metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.${metric.'DP Pool'}.${mname} ${field.value} ${mtimes}\n"
            }
          }
        break
        case 'LU':
          metrics.each { metric ->
            metric.each { field ->
              if (field.key =~ /CTL|LU/) { return }
              if (cfg?.pfm?.ignoreZeroValues && !field?.value?.toInteger()) { return }
              String mname = normalizeMetricName(field.key)
              metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.${metric.LU}.${mname} ${field.value} ${mtimes}\n"
            }
          }
        break
        case 'Cache':
          metrics.each { metric ->
            metric.each { field ->
              if (field.key =~ /CTL|Partition/) { return }
              if (cfg?.pfm?.ignoreZeroValues && !field?.value?.toInteger()) { return }
              String mname = normalizeMetricName(field.key)
              if (metric.containsKey('Partition')) {
                metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.Partition.${metric.Partition}.${mname} ${field.value} ${mtimes}\n"
              } else {
                metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.${mname} ${field.value} ${mtimes}\n"
              }
            }
          }
        break
        case 'Processor':
          metrics.each { metric ->
            metric.each { field ->
              if (field.key =~ /CTL|Core/) { return }
              if (cfg?.pfm?.ignoreZeroValues && !field?.value?.toInteger()) { return }
              String mname = normalizeMetricName(field.key)
              if (metric.containsKey('Core')) {
                metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.Core.${metric.Core}.${mname} ${field.value} ${mtimes}\n"
              } else {
                metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.${mname} ${field.value} ${mtimes}\n"
              }
            }
          }
        break
        case 'Drive':
          metrics.each { metric ->
            metric.each { field ->
              if (field.key =~ /CTL|Unit|HDU/) { return }
              if (cfg?.pfm?.ignoreZeroValues && !field?.value?.toInteger()) { return }
              String mname = normalizeMetricName(field.key)
              metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.${metric.Unit}.${metric.HDU}.${mname} ${field.value} ${mtimes}\n"
            }
          }
        break
        case 'Drive Operate':
          metrics.each { metric ->
            metric.each { field ->
              if (field.key =~ /CTL|Unit|HDU/) { return }
              if (cfg?.pfm?.ignoreZeroValues && !field?.value?.toInteger()) { return }
              String mname = normalizeMetricName(field.key)
              metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.${metric.Unit}.${metric.HDU}.${mname} ${field.value} ${mtimes}\n"
            }
          }
        break
        case 'Backend':
          metrics.each { metric ->
            metric.each { field ->
              if (field.key =~ /CTL|Path/) { return }
              if (cfg?.pfm?.ignoreZeroValues && !field?.value?.toInteger()) { return }
              String mname = normalizeMetricName(field.key)
              metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.${metric.Path}.${mname} ${field.value} ${mtimes}\n"
            }
          }
        break
        case 'Management Area':
          metrics.each { metric ->
            metric.each { field ->
              if (field.key =~ /CTL|Core|DP Pool/) { return }
              if (cfg?.pfm?.ignoreZeroValues && !field?.value?.toInteger()) { return }
              String mname = normalizeMetricName(field.key)
              metricList << "${cfg?.graphite?.prefix}.${data['sn']}.${sectionName.replaceAll(~/[\s\.]/, '')}.${metric.CTL}.${metric.Core}.${metric.'DP Pool'}.${mname} ${field.value} ${mtimes}\n"
            }
          }
        break
        default:
          log.error "Could not build metrics for Section: ${sectionName}"
        break
      }
    }

    Date timeEnd = new Date()
    log.info "Finished Building: ${metricList.size()} Metrics in ${TimeCategory.minus(timeEnd,timeStart)}"
    return metricList
  }

  /**
   * Normalize metric name
   *
   * @param mName String Metric name that sould be Normalized
   * @return String with the Normalized Metric Name
   */
  String normalizeMetricName(String mName){
    switch(mName) {
      case ~/(.*)\(MB\/S\).*/:
        Matcher m = Matcher.lastMatcher
        "${m.group(1)?.trim().replaceAll(~/[\s\.]/, '')}_MBs"
      break
      case ~/(.*)\(IOPS\).*/:
        Matcher m = Matcher.lastMatcher
        "${m.group(1)?.trim().replaceAll(~/[\s\.]/, '')}_IOPS"
      break
      case ~/(.*)\(%\).*/:
        Matcher m = Matcher.lastMatcher
        "${m.group(1)?.trim().replaceAll(~/[\s\.]/, '')}_PCT"
      break
      case ~/(.*)\(microsec.*\).*/:
        Matcher m = Matcher.lastMatcher
        "${m.group(1)?.trim().replaceAll(~/[\s\.]/, '')}_us"
      break
      case ~/(.*)Count\((.*)\).*/:
        Matcher m = Matcher.lastMatcher
        "${m.group(1)?.trim().replaceAll(~/[\s\.]/, '')}_${m.group(2)?.trim().replaceAll(~/<|-$/, '')}_CNT"
      break
      case ~/(.*)Count.*/:
        Matcher m = Matcher.lastMatcher
        "${m.group(1)?.trim().replaceAll(~/[\s\.]/, '')}_CNT"
      break
      case ~/(.*)\((.*)\).*/:
        Matcher m = Matcher.lastMatcher
        "${m.group(1)?.trim().replaceAll(~/[\s\.]/, '')}_${m.group(2)?.trim()}"
      break
      case ~/(.*)/:
        Matcher m = Matcher.lastMatcher
        "${m.group(1)?.trim().replaceAll(~/[\s\.]/, '')}"
      break
      default:
        log.error "Could not normalize the metric: ${mName}"
        mName
      break
    }
  }





  /**
   * Send Traffic Source metrics to the Graphite server
   *
   * @param metrics Interfaces Metric data structure
   */
  void send2Graphite(ArrayList metrics) {
    if (!metrics) { return }

    Date timeStart = new Date()
    int sentCount = 0
    def socket = null

    log.debug "Sending Metrics to Graphite (${cfg?.graphite?.host}:${cfg?.graphite?.port}) using '${cfg?.graphite.protocol}'"

    try {
      if (cfg?.graphite?.protocol?.toLowerCase() == 'tcp'){
        socket = new Socket(cfg?.graphite?.host, cfg?.graphite?.port)
        socket.setSoTimeout(10000)
      } else {
        socket = new DatagramSocket()
        socket.setSoTimeout(10000)
      }

      // Send metrics
      metrics.each { String msg ->
        if (cfg?.graphite?.protocol?.toLowerCase() == 'tcp') {
          Writer writer = new OutputStreamWriter(socket.getOutputStream())
          writer.write(msg)
          writer.flush()
        } else {
          byte[] bytes = msg.getBytes()
          InetAddress addr = InetAddress.getByName(cfg?.graphite?.host)
          DatagramPacket packet = new DatagramPacket(bytes, bytes.length, addr, cfg?.graphite?.port)
          socket.send(packet)
        }
        sentCount++
      }

    } catch (Exception e) {
      StackTraceUtils.deepSanitize(e)
      log.error "Socket exception: ${getStackTrace(e)}"
    } finally {
      socket?.close()
    }

    Date timeEnd = new Date()
    log.info "Finished sending: ${sentCount} Metrics to Graphite in ${TimeCategory.minus(timeEnd, timeStart)}"
  }



  /**
   * Deletes PFM Metric files
   *
   * @param files List of Metric files to delete
   */
  void cleanMetricFiles(ArrayList files){
    if (!files) { return }
    int notDelCount = 0

    files.each {
      Path file = Paths.get(it)

      try {
        Files.delete(file)
      } catch (Exception e) {
        StackTraceUtils.deepSanitize(e)
        log.error "Unable to remove the file: '${file.getFileName()}' : ${getStackTrace(e)}"
        notDelCount++
      }
    }
    log.debug "Removed ${files.size - notDelCount}/${files.size} Metric files"
  }


  /**
   * Moves PFM Metric files to the archive
   *
   * @param archive Archive folder + file folder
   * @param files List of Metric file to move
   */
  void archiveMetricFiles(String archive, ArrayList files){
    if (!files) { return }

    Path folder = Paths.get(archive)
    int notMovedCount = 0

    files.each {
      Path file = Paths.get(it)
      Path folderDevice = file[-2] // File folder
      Path dest = folder.resolve(folderDevice)

      if (!folder.toFile().exists()) {
         log.warn "The metric archive folder: '${folder}' does not exits, going to create it"
         try {
           Files.createDirectories(folder)
         } catch (Exception e) {
           StackTraceUtils.deepSanitize(e)
           log.error "Was unable to create the metric archive folder: '${folder}' : ${getStackTrace(e)}"
         }
       }

      if (!dest.toFile().exists()) {
        log.warn "The metric archive folder: '${dest}' does not exits, going to create it"
         try {
          Files.createDirectories(dest)
        } catch (Exception e) {
          StackTraceUtils.deepSanitize(e)
          log.error "Was unable to create the metric archive folder: '${dest}' : ${getStackTrace(e)}"
        }
      }

      try {
        Files.move(file, dest.resolve(file.getFileName()), REPLACE_EXISTING)
      } catch (Exception e) {
        StackTraceUtils.deepSanitize(e)
        log.error "Unable to move the file: '${file.getFileName()}' to the metric archive folder: '${dest}'! : ${getStackTrace(e)}"
        notMovedCount++
      }
    }
    log.debug "Moved ${files.size - notMovedCount}/${files.size} Metric files to the archive folder"
  }


  /**
   *  Parse Metrics in Parallel
   *
   * @param files List of files to process
   */
  void parseMetrics(ArrayList files) {
    if (!files) { return }
    Date timeStart = new Date()

    GParsPool.withPool() {
      log.info "+ Start Parsing ${files.size} PFM Metric files in parallel using ${PoolUtils.retrieveDefaultPoolSize()} Threads (PoolSize)"
      files.eachParallel { String file ->
        try{
          Map metric = parsePMF(file)
          ArrayList metricList = buildMetrics(metric)
          send2Graphite(metricList)

          if (cfg?.pfm?.metrics_archive) {
            archiveMetricFiles(cfg?.pfm?.metrics_archive,[file])
          } else {
            cleanMetricFiles([file])
          }

        }catch(Exception e){
          StackTraceUtils.deepSanitize(e)
          log.error "Error processing file: ${file} : ${getStackTrace(e)}"
        }
      }
    }
    Date timeEnd = new Date()
    log.info "+ Finished Parsing PFM files in ${TimeCategory.minus(timeEnd,timeStart)}"
  }


  /**
   * Run as daemon the Collecting processe
   *
   */
  void runAsDaemon() {
    try {
      while(true) {
        ArrayList files = findPMFFiles()
        parseMetrics(files)

        System.gc()
        sleep(cfg?.main?.delay*1000)
      }
    } catch (Exception e) {
      StackTraceUtils.deepSanitize(e)
      log.error "runAsDaemon exception: ${getStackTrace(e)}"
      throw new RuntimeException("runAsDaemon exception: ${getStackTrace(e)}")
    }
  }


  /**
   * Main execution loop
   *
   */
  static main(args) {
    HitachiPFM2Graphite main = new HitachiPFM2Graphite()
    addShutdownHook {
      log.info "Shuting down app..."
    }

    try {
      main.runAsDaemon()

    } catch (Exception e) {
      StackTraceUtils.deepSanitize(e)
      log.error "Initialization exception: ${main.getStackTrace(e)}"
      throw new RuntimeException("Initialization exception: ${main.getStackTrace(e)}")
    }
  }
}
