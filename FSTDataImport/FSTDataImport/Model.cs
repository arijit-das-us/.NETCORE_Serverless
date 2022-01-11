using System;
using System.Collections.Generic;
using System.Text;

namespace FSTDataImport
{
    public class FSTSummaryData
    {
        public string RxStatus { get; set; }
        public string TxStatus { get; set; }
        public string DACCount { get; set; }
        public double RxCFGHz { get; set; }
        public double RxDB { get; set; }
        public double TxCFGHz { get; set; }
        public double TxDB { get; set; }
        public double NullCFGHz { get; set; }
        public double NullDB { get; set; }
        public double SourceCurrent { get; set; }
        public double Current { get; set; }
        public double TemperatureInC { get; set; }
        public double Humidity { get; set; }
        public DateTime DateTime { get; set; }
        public string S2pFile { get; set; }
        public string Rx2Status { get; set; }
        public double Rx2CFGHz { get; set; }
        public double Rx2DB { get; set; }

    }
    public class FSTSummary
    {
        public string OriginalFilename { get; set; }
        public string Filename { get; set; }
        public string SoftwareName { get; set; }
        public string SoftwareVersion { get; set; }
        public string DTG { get; set; }
        public string ASMSN { get; set; }
        public string AntennaSN { get; set; }
        public string SegmentSN { get; set; }
        public string SegmentPN { get; set; }
        public string Operator { get; set; }
        public string Location { get; set; }
        public List<String> VNASettings { get; set; }
        public string VNACalibration { get; set; }
        public string ControllerInformation { get; set; }
        public string ConfigFileVersion { get; set; }
        public List<FSTSummaryData> data { get; set; }

    }
}
