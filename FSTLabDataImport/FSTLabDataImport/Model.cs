using System;
using System.Collections.Generic;
using System.Text;

namespace FSTLabDataImport
{
    public class FSTGenericSummaryData
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
        public double GateCurrent { get; set; }
        public string Rx2Status { get; set; }
        public double Rx2CFGHz { get; set; }
        public double Rx2DB { get; set; }
        public double Q1TempC { get; set; }

    }
    public class FSTGenericSummary
    {
        public string OriginalFilename { get; set; }
        public string Filename { get; set; }
        public string SoftwareName { get; set; }
        public string SoftwareVersion { get; set; }
        public DateTime DTG { get; set; }
        public string SegmentSN { get; set; }
        public string SegmentPN { get; set; }
        public string Operator { get; set; }
        public string Location { get; set; }
        public List<String> VNASettings { get; set; }
        public string VNACalibration { get; set; }
        public string ControllerInformation { get; set; }
        public string ConfigFileVersion { get; set; }
        public List<FSTGenericSummaryData> data { get; set; }

    }

    public class IQCLookupData
    {
        public string antennaPN { get; set; }
        public string antennaSN { get; set; }
        public string productSN { get; set; }
        public string serialNumber { get; set; }
        public string revisionNumber { get; set; }
        public string Description { get; set; }
    }

    public class LookupForIQC
    {
        public string originalFilename { get; set; }
        public string location { get; set; }

        public List<IQCLookupData> data { get; set; }

    }
}
