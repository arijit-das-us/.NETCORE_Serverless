using System;
using System.Collections.Generic;
using System.Text;

namespace FeedMeasureData
{
    public class S2PData
    {
        public long Stimulus { get; set; }
        public double RealS11 { get; set; }
        public double ImagS11 { get; set; }
        public double RealS21 { get; set; }
        public double ImagS21 { get; set; }
        public double RealS12 { get; set; }
        public double ImagS12 { get; set; }
        public double RealS22 { get; set; }
        public double ImagS22 { get; set; }
    }
    public class S2P
    {
        public string File { get; set; }
        public List<S2PData> data { get; set; }
        public string Date { get; set; }
        public string Serial { get; set; }
    }

    public class FMConfigData
    {
        public long Row { get; set; }
        public double RxPhi { get; set; }
        public string RxPolType { get; set; }
        public double RxTheta { get; set; }
        public double RxFrequency { get; set; }
        public double RxPol { get; set; }
        public double TxTheta { get; set; }
        public double SettlingTime { get; set; }
        public double TxFrequency { get; set; }
        public double TxPhi { get; set; }
        public double TxPol { get; set; }
        public string TxPolType { get; set; }
    }
    public class FMConfig
    {
        public string File { get; set; }
        public List<FMConfigData> data { get; set; }
        public string Date { get; set; }
        public string Serial { get; set; }
    }

    public class FMSummaryData
    {
        public string Aperture { get; set; }
        public double PatternFreqGHz { get; set; }
        public double S11FreqGHz { get; set; }
        public double S11Gain { get; set; }
        public double S21FreqGHz { get; set; }
        public double S21Gain { get; set; }
        public string S2pFile { get; set; }
        public string Status { get; set; }
    }
    public class FMSummary
    {
        public string OriginalFilename { get; set; }
        public string Serial { get; set; }
        public string ProductionMode { get; set; }
        public string Ticket { get; set; }
        public string Operator { get; set; }
        public string FirmwareVersion { get; set; }
        public string Port { get; set; }
        public string ReceiverStatus { get; set; }
        public List<FMSummaryData> data { get; set; }
        public string Date { get; set; }
        public string File { get; set; }
        public string VnaStartGHz { get; set; }
        public string VnaStopGHz { get; set; }
        public string VnaPoints { get; set; }
        public string VnaCalFile { get; set; }
    }
}
