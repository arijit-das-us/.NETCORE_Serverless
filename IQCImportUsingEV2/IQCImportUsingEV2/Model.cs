using System;
using System.Collections.Generic;
using System.Text;

namespace IQCImportUsingEV2
{
    public class PCDMISIQCGenericSummaryData
    {
        public string dimension { get; set; }
        public string description { get; set; }
        public string feature { get; set; }
        public string axis { get; set; }
        public string segment { get; set; }
        public double nominal { get; set; }
        public double means { get; set; }
        public double plusTol { get; set; }
        public double minusTol { get; set; }
        public double bonus { get; set; }
        public double dev { get; set; }
        public double outTol { get; set; }
        public double devAng { get; set; }
        public double datumShiftEffect { get; set; }
        public double unusedZone { get; set; }
        public double shiftX { get; set; }
        public double shiftY { get; set; }
        public double shiftZ { get; set; }
        public double rotationX { get; set; }
        public double rotationY { get; set; }
        public double rotationZ { get; set; }
        public double min { get; set; }
        public double max { get; set; }
    }
    public class PCDMISGenericSummary
    {
        public string originalFilename { get; set; }
        public string location { get; set; }
        public string ASMSN { get; set; }
        public string pcdmisMeasurementRoutine { get; set; }
        public DateTime DTG { get; set; }
        public string partName { get; set; }
        public string serialNumber { get; set; }
        public string revisionNumber { get; set; }
        public string statisticsCount { get; set; }
        public List<PCDMISIQCGenericSummaryData> data { get; set; }

    }

    public class Measures
    {
        public string measures { get; set; }
        public string serialNumber { get; set; }
    }

    public class DatasheetIQCChecklistData
    {
        public string bubbleNumber { get; set; }
        public string location { get; set; }
        public string target { get; set; }
        public string pulsTol { get; set; }
        public string minusTol { get; set; }
        public string inspectionMethod { get; set; }
        public List<Measures> measures { get; set; }
        /*
        public string sample1 { get; set; }
        public string sample2 { get; set; }
        public string sample3 { get; set; }
        public string sample4 { get; set; }
        public string sample5 { get; set; }
        public string sample6 { get; set; }
        public string sample7 { get; set; }
        public string sample8 { get; set; }
        public string sample9 { get; set; }
        public string sample10 { get; set; }
        public string sample11 { get; set; }
        public string sample12 { get; set; }
        public string sample13 { get; set; }
        public string sample14 { get; set; }
        public string sample15 { get; set; }
        public string sample16 { get; set; }
        public string sample17 { get; set; }
        public string sample18 { get; set; }
        public string sample19 { get; set; }
        public string sample20 { get; set; }
        public string sample21 { get; set; }
        public string sample22 { get; set; }
        public string sample23 { get; set; }
        public string sample24 { get; set; }
        public string sample25 { get; set; }
        public string sample26 { get; set; }
        public string sample27 { get; set; }
        public string sample28 { get; set; }
        public string sample29 { get; set; }
        public string sample30 { get; set; }
        public string sample31 { get; set; }
        public string sample32 { get; set; }
        public string sample33 { get; set; }
        public string sample34 { get; set; }
        public string sample35 { get; set; }
        public string sample36 { get; set; }
        public string sample37 { get; set; }
        public string sample38 { get; set; }
        public string sample39 { get; set; }
        public string sample40 { get; set; }
        public string sample41 { get; set; }
        public string sample42 { get; set; }
        public string sample43 { get; set; }
        public string sample44 { get; set; }
        public string sample45 { get; set; }
        public string sample46 { get; set; }
        public string sample47 { get; set; }
        public string sample48 { get; set; }
        public string sample49 { get; set; }
        public string sample50 { get; set; }
        */

    }
    public class DatasheetIQCChecklist
    {
        public string originalFilename { get; set; }
        public string ASMSN { get; set; }
        public string partDescription { get; set; }
        public string revisionNumber { get; set; }
        public string drawingNumber { get; set; }
        public string lotNumber { get; set; }
        public string supplierName { get; set; }
        public string partNumber { get; set; }
        public string poOrder { get; set; }
        public string quantityReceived { get; set; }
        public string quantityInspected { get; set; }
        public string quantityRejected { get; set; }
        public DateTime dateReceived { get; set; }
        public DateTime dateInspected { get; set; }
        public string inspectedBy { get; set; }
        public List<DatasheetIQCChecklistData> data { get; set; }

    }

    public class IQCLookupData
    {
        public string antennaPN { get; set; }
        public string antennaSN { get; set; }
        public string assemblySN { get; set; }
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
