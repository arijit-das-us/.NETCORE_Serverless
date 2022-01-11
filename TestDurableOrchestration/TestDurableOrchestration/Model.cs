using System;
using System.Collections.Generic;
using System.Text;
using Newtonsoft.Json;

namespace TestDurableOrchestration
{
    public class CsvUserInput
    {
        public string jira { get; set; }
        public string hertzscan_version { get; set; }
        public string auxiliary_angle { get; set; }
        public string mtenna_build_number { get; set; }
        public string test_desg { get; set; }
        public string start_time { get; set; }
    }

    public class Kiru_data{
        public string message { get; set; }  
        public int status { get; set; }
         public List<double> values { get; set; }   
    }
    public class Kiru_testcase{
        public string classname { get; set; }
        public string message { get; set; }
        public string name { get; set; }
        public int status { get; set; }
        public double time { get; set; }
        public Kiru_data data { get; set; }
    }
    public class Kiru_testsuite{
        public int failures { get; set; }
        public string name { get; set; }
        public int status { get; set; }
        public List<Kiru_testcase> testcase { get; set; }
        public int tests { get; set; }
        public double time { get; set; }
    }

    public class Kiru_testsuites{
        public int failures { get; set; }
        public string name { get; set; }
        [JsonProperty("start-time")]
        public string start_time { get; set; }
        [JsonProperty("stop-time")]

        public string stop_time { get; set; }
        public int tests { get; set; }
        public List<Kiru_testsuite> testsuite { get; set; }
        public double time { get; set; }   

    }

    public class Kiru_Data{
        [JsonProperty("antenna-serial-number")]
        public string antenna_serial_number { get; set; }
        [JsonProperty("asm-serial-number")]
        public string asm_serial_number { get; set; }
        public string customer { get; set; }
        [JsonProperty("serial-number")]
        public string serial_number { get; set; }
        public int status { get; set; }
        public List<Kiru_testsuites> testsuites { get; set; }
    }

    //ACPLog
    public class timestamps_Data{
        public int pointing { get; set; } 
    }
    public class patternCommand_Data{
        public double lpa  { get; set; }
        public double phi  { get; set; }
        public double theta  { get; set; }
        public timestamps_Data timestamps  { get; set; }

    }
    public class openamip_Data{
        public int carrierLockState { get; set; } 
        public double dataCnr  { get; set; }
        public double headerCnr  { get; set; }
        public double compPower  { get; set; }
    }

    public class ACPLog_Data{
        public double slsReacqThresh { get; set; }
        
        public double sdDetected { get; set; }
        
        public int state { get; set; }
        public int searchCount { get; set; }
        public int plsyncStick { get; set; }
        public int mispointDetected { get; set; }
         public patternCommand_Data patternCommandRx { get; set; }
        public openamip_Data openamip { get; set; }
        public patternCommand_Data patternCommandTx { get; set; }
        public List<Double> sensors { get; set; }
        public List<Double> ekfData { get; set; }
        public double primaryMetric { get; set; }
        public double secondaryMetric { get; set; }
        public double plEsno { get; set; }
        public double plSync { get; set; }
        public double carrierFrequencyOffsetHz { get; set; }
        public double pointingAccurate { get; set; }
        public Boolean rxLock { get; set; }
        public Boolean wrongSatellite { get; set; }
        public string gpsTime { get; set; }
        public double gpsLat { get; set; }
        public double gpsLon { get; set; }
        public double gpsAlt { get; set; }
        public double gpsQuality { get; set; }
        public double gpsFix { get; set; }
        public double gpsSpeed { get; set; }
        public double gpsDirection { get; set; }
        public int dwell { get; set; }
        public int trkState { get; set; }
        public double gyroBias1 { get; set; }
        public double gyroBias2 { get; set; }
        public double gyroBias3 { get; set; }
        public double PSq1 { get; set; }
        public double PSq2 { get; set; }
        public double PSq3 { get; set; }
        public double PSq4 { get; set; }

    }

    public class log_data{
        public List<ACPLog_Data> log { get; set; }
        public int version { get; set; }
    }

}
