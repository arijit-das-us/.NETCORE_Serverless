using System;
using System.Collections.Generic;
using System.Text;

namespace HertzFarFieldGenericImport
{
    public class HertzFarFieldSummaryData
    {
        public int ScanNumber { get; set; }
        public double Frequency { get; set; }
        public double GaindBi { get; set; }
        public double Xpd { get; set; }
        public double Sidelobe2DBc { get; set; }
        public double IbwHigherGhz { get; set; }
        public double Sidelobe1DBc { get; set; }
        public double IbwLowerGhz { get; set; }
        public double FirstdbIbw { get; set; }
        public double PatternLPA { get; set; }
        public double PatternPHI { get; set; }
        public double PatternTHETA { get; set; }
        public double FrequenciesGHz { get; set; }
        public string OptimizedS21LPA90P0T0 { get; set; }
        public double TheoreticalDirectivity { get; set; }
        public string NF2FF { get; set; }
        public double RXGainFlatnessDb125MHz { get; set; }
        public double RXGainFlatnessDb230_4MHz { get; set; }
        public double TXGainFlatnessDb62_5MHz { get; set; }
        public double TXGainFlatnessDb20MHz { get; set; }
        public double TXGainFlatnessDb20MHzLow { get; set; }
        public double TXGainFlatnessDb20MHzHigh { get; set; }

    }
    public class HertzFarFieldSummary
    {
        public string OriginalFilename { get; set; }
        public string ASMSN { get; set; }
        public string ASMModelNumber { get; set; }
        public DateTime TestDateTime { get; set; }
        public string FileLocation { get; set; }
        public List<HertzFarFieldSummaryData> data { get; set; }

    }
}
