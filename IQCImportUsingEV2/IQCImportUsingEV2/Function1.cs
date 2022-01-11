using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using System.Linq;
using System.Globalization;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using NPOI.HSSF.UserModel;
using NPOI.SS.UserModel;
using MimeKit;
using MailKit.Net.Smtp;


namespace IQCImportUsingEV2
{
    public static class Function1
    {
        [FunctionName("IQCDataForPCDMIS")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("BigDataStorage"));
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("W2sakcsStorage"));
                CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawIQCDataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("IQCDataContainer"));

                string datetime = req.Query["days"];

                string sheaderNOM = req.Headers["days"];


                String datetimeDiff = datetime ?? sheaderNOM ?? "30";

                int dateDiffinNumber = 30;
                try
                {
                    dateDiffinNumber = Int32.Parse(datetimeDiff);
                    Console.WriteLine(dateDiffinNumber);
                }
                catch (FormatException)
                {
                    Console.WriteLine($"Unable to parse number days entered - '{dateDiffinNumber}'");
                }

                DateTime datecutoff = DateTime.Now.AddDays(-dateDiffinNumber);


                log.LogInformation("date time cut off to process IQC data = " + datecutoff);

                BlobContinuationToken blobContinuationToken = null;
                int filecounter = 0;
                do
                {
                    var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                        //prefix: "IQC/EV2",
                        prefix: "IQC",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: blobContinuationToken,
                        options: null,
                        operationContext: null
                    );
                    // load the lookup csv to load the lookup file
                    /*
                    var blobForLookup = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv")) && (Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).Contains("Lookup")));
                    log.LogInformation("blob count total for IQC files=" + blobForLookup.Count());

                    foreach (var blob in blobForLookup)
                    {
                        var block = blob as CloudBlockBlob;

                        var content = await block.DownloadTextAsync();

                        var blobPath = block.Name;
                        var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                        var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                        var sDestination = "EV2/IQC/LookupData/" + block.Name.Replace(".csv", ".json");
                        log.LogInformation("Original abs file path=" + blobPath);
                        log.LogInformation("destination file path=" + sDestination);
                        var jsonObj = GetLookUpData(content, filename, filepath);

                        CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                        var js = JsonConvert.SerializeObject(jsonObj);
                        await cloudBlockBlob.UploadTextAsync(js);
                    }
                    */

                    // Get the value of the continuation token returned by the listing call.
                    
                    blobContinuationToken = resultSegment.ContinuationToken;
                    var blobs = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".CSV") && !Path.GetDirectoryName(b.Uri.AbsolutePath).Contains("OLD"))); //&& (Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("PCDMIS")));
                    log.LogInformation("blob count total for PCDMIS IQC files=" + blobs.Count());

                    foreach (var blob in blobs)
                    {
                        var block = blob as CloudBlockBlob;
                        var last_mod = block.Properties.LastModified.Value.DateTime;
                        if (last_mod >= datecutoff)
                        {

                            var content = await block.DownloadTextAsync();

                            var blobPath = block.Name;
                            var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                            var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                            var sDestination = "U8/IQC/PCDMISData/" + block.Name.Replace(".CSV", ".json");
                            //log.LogInformation("Original abs file path=" + blobPath);
                            //log.LogInformation("destination file path=" + sDestination);
                            var jsonObj = GetPCDMISIQCSummaryData(content, filename, filepath);

                            CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                            var js = JsonConvert.SerializeObject(jsonObj);
                            await cloudBlockBlob.UploadTextAsync(js);
                            filecounter++;
                        }
                    }

                    /*

                    var iqcblobs = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".xls")) && !Path.GetDirectoryName(b.Uri.AbsolutePath).Contains("OLD") && (Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("IQC")));
                    log.LogInformation("blob count total for IQC Datasheet files=" + iqcblobs.Count());

                    foreach (var blob in iqcblobs)
                    {
                        var block = blob as CloudBlockBlob;

                        var content = await block.DownloadTextAsync();

                        var blobPath = block.Name;
                        var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                        var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                        var sDestination = "U8/IQC/Datasheet/" + block.Name.Replace(" ", "_");
                        CloudBlockBlob cloudBlockBlobRaw = container.GetBlockBlobReference(sDestination);
                        log.LogInformation("json blob source=" + block.Name);
                        log.LogInformation("json blob destination=" + sDestination);
                        try
                        {
                            await cloudBlockBlobRaw.StartCopyAsync(new Uri(GetSharedAccessUri(block.Name, rawContainer)));
                            // Display the status of the blob as it is copied
                            ICloudBlob destBlobRef = await container.GetBlobReferenceFromServerAsync(cloudBlockBlobRaw.Name);
                            while (destBlobRef.CopyState.Status == CopyStatus.Pending)
                            {
                                //Console.WriteLine($"Blob: {destBlobRef.Name}, Copied: {destBlobRef.CopyState.BytesCopied ?? 0} of  {destBlobRef.CopyState.TotalBytes ?? 0}");
                                await Task.Delay(500);
                                destBlobRef = await container.GetBlobReferenceFromServerAsync(cloudBlockBlobRaw.Name);
                            }
                            //Console.WriteLine($"Blob: {cloudBlockBlobRaw.Name} Complete");
                        }
                        catch (Exception ex)
                        {
                            log.LogInformation(ex.Message);
                        }
                    }
                    */

                } while (blobContinuationToken != null); // Loop while the continuation token is not null.

                log.LogInformation("Number of PCDMIS IQC Datasheet files processed =" + filecounter);

                return new OkResult();

            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        private static LookupForIQC GetLookUpData(String content, String filename, String filepath)
        {
            string[] lines = content.Split(Environment.NewLine);

            var jsonObj = new LookupForIQC();
            jsonObj.location = filepath;
            jsonObj.originalFilename = filename;

            jsonObj.data = new List<IQCLookupData>();
            bool start = false;
            foreach (var item in lines)
            {
                if (start)
                {
                    var items = item.Split(",", StringSplitOptions.None);
                    if (items.Length > 1)
                    {
                        try
                        {
                            var product_rev = items[2].Split(" ", StringSplitOptions.None);
                            var revisionNM = "";
                            if (product_rev.Length == 2)
                            {
                                revisionNM = product_rev[1];
                            }
                            jsonObj.data.Add(new IQCLookupData
                            {
                                antennaPN = items[0].Trim(),
                                antennaSN = items[1].Trim(),
                                productSN = product_rev[0].Trim(),
                                revisionNumber = revisionNM.Trim(),
                                serialNumber = items[3].Trim(),
                                Description = items[4].Trim()
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception for file " + filepath);
                            Console.WriteLine("exception=" + e.StackTrace);
                            continue;
                        }
                    }
                }
                if (item.StartsWith("Antenna PN,"))
                {
                    start = true;
                }
            }

           return jsonObj;
        }

        private static PCDMISGenericSummary GetPCDMISIQCSummaryData(String content, String filename, String filepath)
        {
            string[] lines = content.Split(Environment.NewLine);
            for (int i=0; i < lines.Length; i++)
            {
                if (lines[i].StartsWith(","))
                    lines[i] = lines[i].Substring(1);
            }


            var jsonObj = new PCDMISGenericSummary();
            jsonObj.originalFilename = filename;
            jsonObj.location = filepath;
            
            try
            {
               
                jsonObj.pcdmisMeasurementRoutine = lines.First(l => l.Contains("PC-DMIS Measurement Routine")).Split(",", StringSplitOptions.None)[1].Trim().Replace("\"","");          
               // jsonObj.partName = lines.First(l => l.Contains("Part Name")).Split(",", StringSplitOptions.None)[1].Trim();
               // jsonObj.revisionNumber = lines.First(l => l.Contains("Revision Number")).Split(",", StringSplitOptions.None)[1].Trim();
               // jsonObj.serialNumber = lines.First(l => l.Contains("Serial Number")).Split(",", StringSplitOptions.None)[1].Trim();
                jsonObj.statisticsCount = lines.First(l => l.Contains("Statistics Count")).Split(",", StringSplitOptions.None)[1].Trim().Replace("\"", "");
            }
            catch
            {
                Console.WriteLine("exception reading metadata for file " + filepath);
                
            }
            
            //jsonObj.ASMSN = filepath.Substring(filepath.LastIndexOf('/') + 1);

            var arrayFilename = filename.Split(new char[] { '_' ,'.'}, StringSplitOptions.None);

            
            if (arrayFilename.Length > 3) {
                if (jsonObj.partName == null || jsonObj.partName.Equals(""))
                {
                    jsonObj.partName = arrayFilename[0];
                }
                if (jsonObj.partName.Length > 13) {
                    
                    if (jsonObj.revisionNumber == null || jsonObj.revisionNumber.Equals(""))
                    {
                        jsonObj.revisionNumber = jsonObj.partName.Substring(14);
                    }
                    jsonObj.partName = jsonObj.partName.Substring(0, 13);
                }
                else
                {
                    if (jsonObj.revisionNumber == null || jsonObj.revisionNumber.Equals(""))
                    {
                        jsonObj.revisionNumber = arrayFilename[1];
                    }
                }
                if (jsonObj.serialNumber == null || jsonObj.serialNumber.Equals(""))
                {
                    jsonObj.serialNumber = arrayFilename[2];
                }
            } else
            {
                if (jsonObj.partName == null || jsonObj.partName.Equals(""))
                {
                    jsonObj.partName = arrayFilename[0];
                }
                if (jsonObj.revisionNumber == null || jsonObj.revisionNumber.Equals(""))
                {
                    jsonObj.revisionNumber = arrayFilename[1].Substring(0,2);
                }
                if (jsonObj.serialNumber == null || jsonObj.serialNumber.Equals(""))
                {
                    jsonObj.serialNumber = arrayFilename[1].Substring(3);
                }

            }
            
            try
            {
                string sdate = lines.First(l => l.StartsWith("Date")).Split(",", StringSplitOptions.None)[1].Replace("\"", "").Trim();
                string stime = lines.First(l => l.StartsWith("Time")).Split(",", StringSplitOptions.None)[1].Replace("\"", "").Trim();
                DateTime dt;
                CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
                DateTimeStyles styles = DateTimeStyles.None;
                DateTime.TryParse(sdate + " " + stime, culture, styles, out dt);

                jsonObj.DTG = dt;
            }
            catch
            {
                Console.WriteLine("exception reading date from metadata for file " + filepath);
                jsonObj.DTG = DateTime.Now;
            }

            jsonObj.data = new List<PCDMISIQCGenericSummaryData>();

            bool start = false;
            var locDimension = -1;
            var locDescription = -1;
            var locFeature = -1;
            var locAxis = -1;
            var locSegment = -1;
            var locNominal = -1;
            var locMeans = -1;
            var locPlusTol = -1;
            var locMinusTol = -1;
            var locBonus = -1;
            var locDev = -1;
            var locOutTol = -1;
            var locDevAng = -1;
            var locDatumShiftEffect = -1;
            var locUnusedZone = -1;
            var locShiftX = -1;
            var locShiftY = -1;
            var locShiftZ = -1;
            var locRotationX = -1;
            var locRotationY = -1;
            var locRotationZ = -1;
            var locMin = -1;
            var locMax = -1;

            foreach (var item in lines)
            {
                
                if (item.StartsWith("DIMENSION,") || item.StartsWith(",,,,") || item.StartsWith("####") || item.StartsWith("CLUSTER"))
                {
                    start = false;
                }
                if (start)
                {
                    var line = item;
                    Regex CSVParser = new Regex(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))");
                    String[] items = CSVParser.Split(item);

                    if (items.Length >1)
                    {
                        try
                        {
                            jsonObj.data.Add(new PCDMISIQCGenericSummaryData
                            {
                                dimension = locDimension!= -1 ? items[locDimension]: null,
                                description = locDescription != -1 ? (items[locDescription].Length > 1250 ? items[locDescription].Substring(0, 1250) : items[locDescription]):null,
                                feature = locFeature != -1 ? items[locFeature] : null,
                                axis = locAxis != -1 ? items[locAxis] : null,
                                segment = locSegment != -1 ? items[locSegment] : null,
                                nominal = double.Parse(locNominal != -1 ? (items[locNominal].Equals("") ? "0.0" : items[locNominal]):"0.0"),
                                means = double.Parse(locMeans != -1 ? (items[locMeans].Equals("") ? "0.0" : items[locMeans]) : "0.0"),
                                plusTol = double.Parse(locPlusTol != -1 ? (items[locPlusTol].Equals("") ? "0.0" : items[locPlusTol]) : "0.0"),
                                minusTol = double.Parse(locMinusTol != -1 ? (items[locMinusTol].Equals("") ? "0.0" : items[locMinusTol]) : "0.0"),
                                bonus = double.Parse(locBonus != -1 ? (items[locBonus].Equals("") ? "0.0" : items[locBonus]) : "0.0"),
                                dev = double.Parse(locDevAng != -1 ? (items[locDevAng].Equals("") ? "0.0" : items[locDevAng]) : "0.0"),
                                outTol = double.Parse(locOutTol != -1 ? (items[locOutTol].Equals("") ? "0.0" : items[locOutTol]) : "0.0"),
                                devAng = double.Parse(locDevAng != -1 ? (items[locDevAng].Equals("") ? "0.0" : items[locDevAng]) : "0.0"),
                                datumShiftEffect = double.Parse(locDatumShiftEffect != -1 ? (items[locDatumShiftEffect].Equals("") ? "0.0" : items[locDatumShiftEffect]) : "0.0"),
                                unusedZone = double.Parse(locUnusedZone != -1 ? (items[locUnusedZone].Equals("") ? "0.0" : items[locUnusedZone]) : "0.0"),
                                shiftX = double.Parse(locShiftX != -1 ? (items[locShiftX].Equals("") ? "0.0" : items[locShiftX]) : "0.0"),
                                shiftY = double.Parse(locShiftY != -1 ? (items[locShiftY].Equals("") ? "0.0" : items[locShiftY]) : "0.0"),
                                shiftZ = double.Parse(locShiftZ != -1 ? (items[locShiftZ].Equals("") ? "0.0" : items[locShiftZ]) : "0.0"),
                                rotationX = double.Parse(locRotationX != -1 ? (items[locRotationX].Equals("") ? "0.0" : items[locRotationX]) : "0.0"),
                                rotationY = double.Parse(locRotationY != -1 ? (items[locRotationY].Equals("") ? "0.0" : items[locRotationY]) : "0.0"),
                                rotationZ = double.Parse(locRotationZ != -1 ? (items[locRotationZ].Equals("") ? "0.0" : items[locRotationZ]) : "0.0"),
                                min = double.Parse(locMin != -1 ? (items[locMin].Equals("") ? "0.0" : items[locMin]) : "0.0"),
                                max = double.Parse(locMax != -1 ? (items[locMax].Equals("") ? "0.0" : items[locMax]) : "0.0")
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception for file " + filepath);
                            Console.WriteLine("exception=" + e.StackTrace);
                            Console.WriteLine("items=" + items);
                            continue;
                        }
                    }
                }
                if (item.StartsWith("DIMENSION,"))
                {
                    start = true;
                    locDimension = -1;
                    locDescription = -1;
                    locFeature = -1;
                    locAxis = -1;
                    locSegment = -1;
                    locNominal = -1;
                    locMeans = -1;
                    locPlusTol = -1;
                    locMinusTol = -1;
                    locBonus = -1;
                    locDev = -1;
                    locOutTol = -1;
                    locDevAng = -1;
                    locDatumShiftEffect = -1;
                    locUnusedZone = -1;
                    locShiftX = -1;
                    locShiftY = -1;
                    locShiftZ = -1;
                    locRotationX = -1;
                    locRotationY = -1;
                    locRotationZ = -1;
                    locMin = -1;
                    locMax = -1;
                    
                    var items = item.Split(",", StringSplitOptions.None);
                    var location = -1;
                    foreach (var col in items)
                    {
                        switch (col.Replace("\"","").Trim())
                        {
                            case "DIMENSION":
                                locDimension = ++location;
                                break;
                            case "DESCRIPTION":
                                locDescription = ++location;
                                break;
                            case "FEATURE":
                                locFeature = ++location;
                                break;
                            case "AXIS":
                                locAxis = ++location;
                                break;
                            case "SEGMENT":
                                locSegment = ++location;
                                break;
                            case "NOMINAL":
                                locNominal = ++location;
                                break;
                            case "MEAS":
                                locMeans = ++location;
                                break;
                            case "+TOL":
                                locPlusTol = ++location;
                                break;
                            case "TOL":
                                locPlusTol = ++location;
                                break;
                            case "-TOL":
                                locMinusTol = ++location;
                                break;
                            case "BONUS":
                                locBonus = ++location;
                                break;
                            case "DEV":
                                locDev = ++location;
                                break;
                            case "OUTTOL":
                                locOutTol = ++location;
                                break;
                            case "DEVANG":
                                locDevAng = ++location;
                                break;
                            case "DATUM SHIFT EFFECT":
                                locDatumShiftEffect = ++location;
                                break;
                            case "UNUSED ZONE":
                                locUnusedZone = ++location;
                                break;
                            case "SHIFT X":
                                locShiftX = ++location;
                                break;
                            case "SHIFT Y":
                                locShiftY = ++location;
                                break;
                            case "SHIFT Z":
                                locShiftZ = ++location;
                                break;
                            case "ROTATION X":
                                locRotationX = ++location;
                                break;
                            case "ROTATION Y":
                                locRotationY = ++location;
                                break;
                            case "ROTATION Z":
                                locRotationZ = ++location;
                                break;
                            case "MIN":
                                locMin = ++location;
                                break;
                            case "MAX":
                                locMax = ++location;
                                break;
                            case "":
                                break;
                            default:
                                Console.WriteLine("Default case, could not find column="+col);
                                break;
                        }
                    }
                }


            }

            return jsonObj;
        }

        // Create a SAS token for the source blob, to enable it to be read by the StartCopyAsync method
        private static string GetSharedAccessUri(string blobName, CloudBlobContainer container)
        {
            DateTime toDateTime = DateTime.Now.AddMinutes(60);

            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy
            {
                Permissions = SharedAccessBlobPermissions.Read,
                SharedAccessStartTime = null,
                SharedAccessExpiryTime = new DateTimeOffset(toDateTime)
            };

            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            string sas = blob.GetSharedAccessSignature(policy);

            return blob.Uri.AbsoluteUri + sas;
        }
    }

    public static class Function2
    {
        [FunctionName("IQCLookUpDV")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("BigDataStorage"));
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("W2sakcsStorage"));
                CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawIQCDataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("IQCDataContainer"));

                /*
                string datetime = req.Query["yearAndMonth"];

                string sheaderNOM = req.Headers["yearAndMonth"];


                datetime = datetime ?? sheaderNOM ?? DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString();
                

                log.LogInformation("date time = " + datetime);
                */

                var resultSegmentParts = await rawContainer.ListBlobsSegmentedAsync(
                        //prefix: "IQC/EV2",
                        prefix: "IQC/parts",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: null,
                        options: null,
                        operationContext: null
                    );
                // load the lookup csv to load the lookup file


                var blobForParts = resultSegmentParts.Results.Where(b => (Path.GetFileName(b.Uri.AbsolutePath).Equals("parts.txt")));
                log.LogInformation("blob count total for parts files=" + blobForParts.Count());
                string[] parts = null;

                foreach (var blob in blobForParts)
                {
                    var block = blob as CloudBlockBlob;

                    var content = await block.DownloadTextAsync();
                    parts = content.Split(Environment.NewLine);
                }
                Console.WriteLine("List of parts");
                foreach (var a in parts)
                {
                    Console.WriteLine(a);
                }

                var resultSegmentAggLinkDB = await rawContainer.ListBlobsSegmentedAsync(
                        prefix: "IQC/linkingdb/aggregated",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: null,
                        options: null,
                        operationContext: null
                    );
                // load the lookup csv to load the lookup file


                var blobForAggLinkDB = resultSegmentAggLinkDB.Results.Where(b => (Path.GetFileName(b.Uri.AbsolutePath).EndsWith(".csv")));
                log.LogInformation("blob count total for Agg LinkDB=" + blobForAggLinkDB.Count());
                string AggLinkDBCSV = null;
                string sLastAggDate = "20000101";
                var sDestination = "U8/IQC/LookupData/IQC/linkingdb.json";
                var sDestinationForemail = "U8/IQC/LookupData/email";
                string sourceFilename = "linkingdb_637498955362486000.csv";
                string sourceFilepath = "IQC/linkingdb/aggregated";
                log.LogInformation("destination file path=" + sDestination);
               

                foreach (var blob in blobForAggLinkDB)
                {
                    var block = blob as CloudBlockBlob;
                    var blobPath = block.Name;
                    sourceFilename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    sourceFilepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    sLastAggDate = sourceFilename.Substring(10, sourceFilename.IndexOf(".")-10);
                    log.LogInformation("Original aggregated file path=" + blobPath);
                    AggLinkDBCSV = await block.DownloadTextAsync();

                    //await block.DeleteAsync();

                }
                log.LogInformation("Last aggregated date=" + sLastAggDate);


                DateTime lastDate = new DateTime(long.Parse(sLastAggDate), DateTimeKind.Utc).ToLocalTime();
                DateTime currDate = DateTime.Now;
                int dateDiff = currDate.DayOfYear - lastDate.DayOfYear;
                if(dateDiff > 1)
                {
                    string emailText = "There is gap found in the linking DB extraction, last date when the utility was executed is " + lastDate.ToString("MMMM dd, yyyy");
                    CloudBlockBlob cloudBlockBlobemail = container.GetBlockBlobReference(sDestinationForemail + "/" + DateTime.UtcNow.Ticks + ".txt");
                    await cloudBlockBlobemail.UploadTextAsync(emailText);
                }

                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                        //prefix: "IQC/EV2",
                        prefix: "IQC/linkingdb",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: blobContinuationToken,
                        options: null,
                        operationContext: null
                    );
                    // load the lookup csv to load the lookup file
                    
                    
                    

                    var blobForchangeslinkDBFiles = resultSegment.Results.Where(b => (Path.GetFileName(b.Uri.AbsolutePath).EndsWith(".csv") && (!Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("linkingdb")) && (Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).CompareTo(sLastAggDate) > 0) ));
                    log.LogInformation("blob count total for linking db files=" + blobForchangeslinkDBFiles.Count());

                    foreach (var blob in blobForchangeslinkDBFiles)
                    {
                        var block = blob as CloudBlockBlob;

                        if (block.Properties.Length == 0)
                        {
                            var blobPath = block.Name;
                            log.LogInformation("zero length linking db files=" + block.Name);
                            var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                            var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                            CloudBlockBlob cloudBlockBlobemail = container.GetBlockBlobReference(sDestinationForemail + "/" + DateTime.UtcNow.Ticks + ".txt");
                            var sTciks = filename.Substring(0, filename.LastIndexOf('.'));
                            DateTime myDate = new DateTime(long.Parse(sTciks), DateTimeKind.Utc).ToLocalTime();
                            String sDate = myDate.ToString("MMMM dd, yyyy");
                            string emailText = "Zero file generated for the day " + sDate;
                            await cloudBlockBlobemail.UploadTextAsync(emailText);
                            //sendEmail(emailText);

                        }
                        else
                        {
                            var content = await block.DownloadTextAsync();

                            var blobPath = block.Name;
                            var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                            var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));

                            log.LogInformation("Original abs file path=" + blobPath);

                            AggLinkDBCSV = content.Substring(content.IndexOf(Environment.NewLine) +2) + AggLinkDBCSV ;

                        }

                    }


                } while (blobContinuationToken != null); // Loop while the continuation token is not null.

                

                log.LogInformation("calling GetLookUpDataFromLinkDB");
                var newSourceFile = "linkingdb_" + DateTime.UtcNow.Ticks + ".csv";
                var jsonObj = GetLookUpDataFromLinkDB(AggLinkDBCSV, newSourceFile, parts);

                CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                var js = JsonConvert.SerializeObject(jsonObj);
                await cloudBlockBlob.UploadTextAsync(js);

                string sWritePath = sourceFilepath + "/" + newSourceFile;
                log.LogInformation("uploading new aggregated file=" + sWritePath);
                CloudBlockBlob cloudBlobrawcontainer = rawContainer.GetBlockBlobReference(sWritePath);
                await cloudBlobrawcontainer.UploadTextAsync(AggLinkDBCSV);

                log.LogInformation("before deleting the old aggregated file");

                foreach (var blob in blobForAggLinkDB)
                {
                    var block = blob as CloudBlockBlob;
                    var blobPath = block.Name;
                    log.LogInformation("deleting the old aggregated file =" + blobPath);
                    await block.DeleteAsync();

                }

                return new OkResult();

            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        private static LookupForIQC GetLookUpData(String content, String filename, String filepath, string[] asm, string[] parts)
        {
            string[] lines = content.Split('\n');

            Dictionary<string, List<string[]>> dict = new Dictionary<string, List<String[]>>();
           
            var jsonObj = new LookupForIQC();
            jsonObj.location = filepath;
            jsonObj.originalFilename = filename;

            jsonObj.data = new List<IQCLookupData>();
            //bool start = false;
            foreach (var line in lines)
            {
             //   Console.WriteLine("line="+line);
                if (!line.Equals(""))
                {
                    var items = line.Split(",", StringSplitOptions.None);
              //      Console.WriteLine("key=" + items[1]);
                    var keyUp = items[1].ToUpperInvariant();
               //     Console.WriteLine("key up=" + keyUp);
                    var partno = items[2];
                    var serial = items[3].ToUpperInvariant();
                    var rev = "";
                    var segment = "";
                    if (items[2].Length > 13)
                    {
                        partno = items[2].Substring(0, 13);
                    }
                    if (items[2].Length >= 16)
                    {
                        rev = items[2].Substring(14, 2);
                    }
                    if (partno.Equals("830-00325-002") && items[2].Length > 17)
                    {
                        segment = items[2].Substring(17);
                    }

                    if (dict.ContainsKey(keyUp))
                    {
                        var l = dict[keyUp];
                        var check = true;
                        
                        foreach (var i in l)
                        {
                            if (i[0].Equals(partno) && i[1].Equals(serial) && i[2].Equals(rev))
                            {
                                check = false;
                            }
                        } 
                        if (check)
                        {
                            l.Add(new string[] { partno, serial, rev, segment });
                            dict[keyUp] = l;
                        }
                    }
                    else
                    {
                        List<string[]> l = new List<string[]>();
                        l.Add(new string[] { partno, serial, rev, segment });
                        dict.Add(keyUp, l);
                    }
                }
                
            }

            Console.WriteLine("end of dic population");

            foreach (var pair in dict)
            {
               // if (pair.Key.Equals("ABW000K200826011")) {
                //    Console.WriteLine("key=" + pair.Key);
                    foreach (var v in pair.Value) {
                        Console.WriteLine("key=" + pair.Key + ",Link type=" + v[0] + ",Linked Serial Number=" + v[1]);
                    }
             //   }
            }


            HashSet<string> hs = new HashSet<string>();
            foreach(var i in parts)
            {

                hs.Add(i); 
            }
            Console.WriteLine("Now running population job");

            foreach (var serialno in asm)
            {
                if (dict.ContainsKey(serialno)) {
                 //   Console.WriteLine("json object length=" + jsonObj.data.Count);
                    var list_a = dict[serialno];
                    foreach (var a in list_a)
                    {
                        if (a[1].StartsWith("U8.2ASA"))
                        {
                            var list_b = dict[a[1]];
                            foreach (var b in list_b)
                            {
                                
                                if (hs.Contains(b[0]))
                                {
                                    jsonObj.data.Add(new IQCLookupData
                                    {
                                        antennaSN = serialno.Trim(),
                                        productSN = b[0].Trim(),
                                        revisionNumber = b[2].Trim(),
                                        serialNumber = b[1].Trim(),
                                        Description = "IQC"
                                    });
                                }
                                if (b[1].StartsWith("U8.2APR"))
                                {
                                    var list_c = dict[b[1]];
                                    foreach (var c in list_c)
                                    {
                                        if (hs.Contains(c[0]) )
                                        {
                                            if (c[0].Equals("830-00325-002")) {
                                                jsonObj.data.Add(new IQCLookupData
                                                {
                                                    antennaSN = serialno.Trim(),
                                                    productSN = c[0].Trim(),
                                                    revisionNumber = c[2].Trim(),
                                                    serialNumber = c[1].Trim(),
                                                    Description = "FST"
                                                });
                                            }
                                            else
                                            {
                                                jsonObj.data.Add(new IQCLookupData
                                                {
                                                    antennaSN = serialno.Trim(),
                                                    productSN = c[0].Trim(),
                                                    revisionNumber = c[2].Trim(),
                                                    serialNumber = c[1].Trim(),
                                                    Description = "IQC"
                                                });
                                            }
                                        }
                                        if (c[1].StartsWith("U8.2FED"))
                                        {
                                            var list_d = dict[c[1]];
                                            foreach (var d in list_d)
                                            {
                                                if (hs.Contains(d[0]))
                                                {
                                                    jsonObj.data.Add(new IQCLookupData
                                                    {
                                                        antennaSN = serialno.Trim(),
                                                        productSN = d[0].Trim(),
                                                        revisionNumber = d[2].Trim(),
                                                        serialNumber = d[1].Trim(),
                                                        Description = "IQC"
                                                    });
                                                }
                                                if (d[1].StartsWith("U8.2LCH"))
                                                {
                                                    var list_e = dict[d[1]];
                                                    foreach (var e in list_e)
                                                    {
                                                        if (hs.Contains(e[0]))
                                                        {
                                                            jsonObj.data.Add(new IQCLookupData
                                                            {
                                                                antennaSN = serialno.Trim(),
                                                                productSN = e[0].Trim(),
                                                                revisionNumber = e[2].Trim(),
                                                                serialNumber = e[1].Trim(),
                                                                Description = "IQC"
                                                            });
                                                        }
                                                    }
                                                }

                                            }
                                        }
                                    }
                                }
                            }
                        }
                     }
                    /*
                    jsonObj.data.Add(new IQCLookupData
                    {
                        antennaSN = serialno.Trim(),
                        productSN = a[0].Trim().Substring(0,13),
                        revisionNumber = revisionNM.Trim(),
                        serialNumber = a[1].Trim(),
                        Description = items[4].Trim()
                    }); */
                }
            }
            /*
                foreach (var item in lines)
            {
                if (start)
                {
                    var items = item.Split(",", StringSplitOptions.None);
                    if (items.Length > 1)
                    {
                        try
                        {
                            var product_rev = items[2].Split(" ", StringSplitOptions.None);
                            var revisionNM = "";
                            if (product_rev.Length == 2)
                            {
                                revisionNM = product_rev[1];
                            }
                            jsonObj.data.Add(new IQCLookupData
                            {
                                antennaPN = items[0].Trim(),
                                antennaSN = items[1].Trim(),
                                productSN = product_rev[0].Trim(),
                                revisionNumber = revisionNM.Trim(),
                                serialNumber = items[3].Trim(),
                                Description = items[4].Trim()
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception for file " + filepath);
                            Console.WriteLine("exception=" + e.StackTrace);
                            continue;
                        }
                    }
                }
                if (item.StartsWith("Antenna PN,"))
                {
                    start = true;
                }
            }
            */

            Console.WriteLine("json object length=" + jsonObj.data.Count);

            return jsonObj;
        }

        private static LookupForIQC GetLookUpDataNew(String content, String filename, String filepath, string[] asm, string[] parts)
        {
            string[] lines = content.Split('\n');

            Dictionary<string, List<string[]>> dict = new Dictionary<string, List<String[]>>();

            Dictionary<String, String> dictASMLevelALL = new Dictionary<String, String>();
            Dictionary<String, String> dictASMLevel1 = new Dictionary<String, String>();
            Dictionary<String, String> dictASMLevel2 = new Dictionary<String, String>();
            Dictionary<String, String[]> dictASMLevel3 = new Dictionary<String, String[]>();
            Dictionary<String, String[]> dictASMLevel4 = new Dictionary<String, String[]>();
            Dictionary<String, String[]> dictASMLevel5 = new Dictionary<String, String[]>();

            HashSet<string> hsParts = new HashSet<string>();
            HashSet<string> hsASM = new HashSet<string>();
            foreach (var i in parts)
            {

                hsParts.Add(i);
            }

            var jsonObj = new LookupForIQC();
            jsonObj.location = filepath;
            jsonObj.originalFilename = filename;

            jsonObj.data = new List<IQCLookupData>();
            //bool start = false;
            foreach (var line in lines)
            {
                //   Console.WriteLine("line="+line);
                if (!line.Equals(""))
                {
                    var items = line.Split(",", StringSplitOptions.None);
                    var AssmeblyUp = items[0].ToUpperInvariant().Trim();
                    //      Console.WriteLine("key=" + items[1]);
                    var SerialNumberUp = items[1].ToUpperInvariant().Trim();
                    //     Console.WriteLine("key up=" + keyUp);
                    var LinkedTypeUp = items[2].ToUpperInvariant().Trim();
                    var LinkedSerialNumberUp = items[3].ToUpperInvariant().Trim();

                    if (AssmeblyUp.Length >= 13)  //assembly for ASM numbers
                    {
                        if (!string.IsNullOrEmpty(LinkedSerialNumberUp))
                        {
                            
                            if (!dictASMLevel1.ContainsKey(LinkedSerialNumberUp))
                            {
                                dictASMLevelALL.Add(LinkedSerialNumberUp, SerialNumberUp);
                            }
                        }
                    }

                    if (AssmeblyUp.Length >= 13 && AssmeblyUp.Substring(0,13).Equals("810-00061-000"))  //assembly for ASM numbers
                    {
                        if (!string.IsNullOrEmpty(LinkedSerialNumberUp))
                        {
                            hsASM.Add(SerialNumberUp);
                            if (!dictASMLevel1.ContainsKey(LinkedSerialNumberUp))
                            {
                                dictASMLevel1.Add(LinkedSerialNumberUp, SerialNumberUp);
                            }
                        }
                    } else if (AssmeblyUp.Length >= 13 && AssmeblyUp.Substring(0, 13).Equals("840-00036-000"))
                    {
                        if (!string.IsNullOrEmpty(LinkedSerialNumberUp))
                        {
                            if (!dictASMLevel2.ContainsKey(LinkedSerialNumberUp))
                            {
                                dictASMLevel2.Add(LinkedSerialNumberUp, SerialNumberUp);
                            }
                        }
                    }
                    else if (AssmeblyUp.Length >= 13 && AssmeblyUp.Substring(0, 13).Equals("820-00538-000"))
                    {
                        if (!string.IsNullOrEmpty(LinkedSerialNumberUp))
                        {
                            String[] str = new String[3];
                            str[0] = AssmeblyUp; str[1] = SerialNumberUp; str[2] = LinkedTypeUp;
                            if (!dictASMLevel3.ContainsKey(LinkedSerialNumberUp))
                            {
                                dictASMLevel3.Add(LinkedSerialNumberUp, str);
                            }
                        }
                    }
                    else if (AssmeblyUp.Length >= 13 && AssmeblyUp.Substring(0, 13).Equals("820-00537-000"))
                    {
                        if (!string.IsNullOrEmpty(LinkedSerialNumberUp))
                        {
                            String[] str = new String[3];
                            str[0] = AssmeblyUp; str[1] = SerialNumberUp; str[2] = LinkedTypeUp;
                            if (!dictASMLevel4.ContainsKey(LinkedSerialNumberUp)) {
                                dictASMLevel4.Add(LinkedSerialNumberUp, str);
                            }
                        }
                    }
                    else if (AssmeblyUp.Length >= 13 && AssmeblyUp.Substring(0, 13).Equals("820-00550-000"))
                    {
                        if (!string.IsNullOrEmpty(LinkedSerialNumberUp))
                        {
                            String[] str = new String[3];
                            str[0] = AssmeblyUp; str[1] = SerialNumberUp; str[2] = LinkedTypeUp;
                            if (!dictASMLevel5.ContainsKey(LinkedSerialNumberUp)) {
                                dictASMLevel5.Add(LinkedSerialNumberUp, str);
                            }

                        }
                    }
                }

            }

            Console.WriteLine("size of dictASMLevel1=" + dictASMLevel1.Count);
            Console.WriteLine("size of dictASMLevel2=" + dictASMLevel2.Count);
            Console.WriteLine("size of dictASMLevel3=" + dictASMLevel3.Count);
            Console.WriteLine("size of dictASMLevel4=" + dictASMLevel4.Count);
            Console.WriteLine("size of dictASMLevel5=" + dictASMLevel5.Count);





            foreach (var item in dictASMLevel4)
            {
                Console.WriteLine("820-00537-000 linked=" + item.Key);
                String ASMNumber = getFromDictioary(item.Value[1], dictASMLevel1);
                String partno="";
                String rev="";
                
                if (item.Value[2].Length == 13)
                {
                    partno = item.Value[2];
                }
                if (item.Value[2].Length >= 16)
                {
                    partno = item.Value[2].Substring(0, 13);
                    rev = item.Value[2].Substring(14, 2);
                }
                if (hsParts.Contains(partno) && !string.IsNullOrEmpty(ASMNumber)) {
                    Console.WriteLine("adding to json ASM="+ ASMNumber + " partno="+ partno + " serialNumber="+ item.Key);
                    jsonObj.data.Add(new IQCLookupData
                    {
                        antennaSN = ASMNumber,
                        productSN = partno,
                        revisionNumber = rev,
                        serialNumber = item.Key,
                        Description = "IQC"
                    });
                }
            }

            Console.WriteLine("size of json object after 820-00537-000 =" + jsonObj.data.Count);

            foreach (var item in dictASMLevel3)
            {
                Console.WriteLine("820-00538-000 linked=" + item.Key);
                String ASMNumber = "";
                var value1 = getFromDictioary2(item.Value[1], dictASMLevel4);
                if (value1 != null)
                    ASMNumber = getFromDictioary(value1[1], dictASMLevel1);
                String partno = "";
                String rev = "";
                if (item.Value[2].Length == 13)
                {
                    partno = item.Value[2];
                }
                if (item.Value[2].Length >= 16)
                {
                    partno = item.Value[2].Substring(0, 13);
                    rev = item.Value[2].Substring(14, 2);
                }
                if (hsParts.Contains(partno))
                {
                    Console.WriteLine("adding to json ASM=" + ASMNumber + " partno=" + partno + " serialNumber=" + item.Key);
                    jsonObj.data.Add(new IQCLookupData
                    {
                        antennaSN = ASMNumber,
                        productSN = partno,
                        revisionNumber = rev,
                        serialNumber = item.Key,
                        Description = "IQC"
                    });
                }
            }

            Console.WriteLine("size of json object after 820-00538-000 =" + jsonObj.data.Count);

            foreach (var item in dictASMLevel5)
            {
                Console.WriteLine("820-00550-000 linked=" + item.Key);
                String ASMNumber = "";
                var value1 = getFromDictioary2(item.Value[1], dictASMLevel3);
                if (value1 != null)
                {
                    var value2 = getFromDictioary2(value1[1], dictASMLevel4);
                    if (value2 != null)
                        ASMNumber = getFromDictioary(value2[1], dictASMLevel1);
                }

                String partno = "";
                String rev = "";
                if (item.Value[2].Length == 13)
                {
                    partno = item.Value[2];
                }
                if (item.Value[2].Length >= 16)
                {
                    partno = item.Value[2].Substring(0, 13);
                    rev = item.Value[2].Substring(14, 2);
                }
                if (hsParts.Contains(partno))
                {
                    Console.WriteLine("adding to json ASM=" + ASMNumber + " partno=" + partno + " serialNumber=" + item.Key);
                    jsonObj.data.Add(new IQCLookupData
                    {
                        antennaSN = ASMNumber,
                        productSN = partno,
                        revisionNumber = rev,
                        serialNumber = item.Key,
                        Description = "IQC"
                    });
                }
            }

            Console.WriteLine("size of json object after 820-00550-000 =" + jsonObj.data.Count);

            /*

            var rev = "";
            var segment = "";
            if (items[2].Length > 13)
            {
                partno = items[2].Substring(0, 13);
            }
            if (items[2].Length >= 16)
            {
                rev = items[2].Substring(14, 2);
            }
            if (partno.Equals("830-00325-002") && items[2].Length > 17)
            {
                segment = items[2].Substring(17);
            }

            if (dict.ContainsKey(keyUp))
            {
                var l = dict[keyUp];
                var check = true;

                foreach (var i in l)
                {
                    if (i[0].Equals(partno) && i[1].Equals(serial) && i[2].Equals(rev))
                    {
                        check = false;
                    }
                }
                if (check)
                {
                    l.Add(new string[] { partno, serial, rev, segment });
                    dict[keyUp] = l;
                }
            }
            else
            {
                List<string[]> l = new List<string[]>();
                l.Add(new string[] { partno, serial, rev, segment });
                dict.Add(keyUp, l);
            }
        }

    }

    Console.WriteLine("end of dic population");

    foreach (var pair in dict)
    {
        // if (pair.Key.Equals("ABW000K200826011")) {
        //    Console.WriteLine("key=" + pair.Key);
        foreach (var v in pair.Value)
        {
            Console.WriteLine("key=" + pair.Key + ",Link type=" + v[0] + ",Linked Serial Number=" + v[1]);
        }
        //   }
    }



    Console.WriteLine("Now running population job");

    foreach (var serialno in asm)
    {
        if (dict.ContainsKey(serialno))
        {
            //   Console.WriteLine("json object length=" + jsonObj.data.Count);
            var list_a = dict[serialno];
            foreach (var a in list_a)
            {
                if (a[1].StartsWith("U8.2ASA"))
                {
                    var list_b = dict[a[1]];
                    foreach (var b in list_b)
                    {

                        if (hs.Contains(b[0]))
                        {
                            jsonObj.data.Add(new IQCLookupData
                            {
                                antennaSN = serialno.Trim(),
                                productSN = b[0].Trim(),
                                revisionNumber = b[2].Trim(),
                                serialNumber = b[1].Trim(),
                                Description = "IQC"
                            });
                        }
                        if (b[1].StartsWith("U8.2APR"))
                        {
                            var list_c = dict[b[1]];
                            foreach (var c in list_c)
                            {
                                if (hs.Contains(c[0]))
                                {
                                    if (c[0].Equals("830-00325-002"))
                                    {
                                        jsonObj.data.Add(new IQCLookupData
                                        {
                                            antennaSN = serialno.Trim(),
                                            productSN = c[0].Trim(),
                                            revisionNumber = c[2].Trim(),
                                            serialNumber = c[1].Trim(),
                                            Description = "FST"
                                        });
                                    }
                                    else
                                    {
                                        jsonObj.data.Add(new IQCLookupData
                                        {
                                            antennaSN = serialno.Trim(),
                                            productSN = c[0].Trim(),
                                            revisionNumber = c[2].Trim(),
                                            serialNumber = c[1].Trim(),
                                            Description = "IQC"
                                        });
                                    }
                                }
                                if (c[1].StartsWith("U8.2FED"))
                                {
                                    var list_d = dict[c[1]];
                                    foreach (var d in list_d)
                                    {
                                        if (hs.Contains(d[0]))
                                        {

                                        }
                                        if (d[1].StartsWith("U8.2LCH"))
                                        {
                                            var list_e = dict[d[1]];
                                            foreach (var e in list_e)
                                            {
                                                if (hs.Contains(e[0]))
                                                {
                                                    jsonObj.data.Add(new IQCLookupData
                                                    {
                                                        antennaSN = serialno.Trim(),
                                                        productSN = e[0].Trim(),
                                                        revisionNumber = e[2].Trim(),
                                                        serialNumber = e[1].Trim(),
                                                        Description = "IQC"
                                                    });
                                                }
                                            }
                                        }

                                    }
                                }
                            }
                        }
                    }
                }
            }
            /*
            jsonObj.data.Add(new IQCLookupData
            {
                antennaSN = serialno.Trim(),
                productSN = a[0].Trim().Substring(0,13),
                revisionNumber = revisionNM.Trim(),
                serialNumber = a[1].Trim(),
                Description = items[4].Trim()
            }); */

            /*
                foreach (var item in lines)
            {
                if (start)
                {
                    var items = item.Split(",", StringSplitOptions.None);
                    if (items.Length > 1)
                    {
                        try
                        {
                            var product_rev = items[2].Split(" ", StringSplitOptions.None);
                            var revisionNM = "";
                            if (product_rev.Length == 2)
                            {
                                revisionNM = product_rev[1];
                            }
                            jsonObj.data.Add(new IQCLookupData
                            {
                                antennaPN = items[0].Trim(),
                                antennaSN = items[1].Trim(),
                                productSN = product_rev[0].Trim(),
                                revisionNumber = revisionNM.Trim(),
                                serialNumber = items[3].Trim(),
                                Description = items[4].Trim()
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception for file " + filepath);
                            Console.WriteLine("exception=" + e.StackTrace);
                            continue;
                        }
                    }
                }
                if (item.StartsWith("Antenna PN,"))
                {
                    start = true;
                }
            }
            */

            Console.WriteLine("json object length=" + jsonObj.data.Count);

            return jsonObj;
        }

        private static void sendEmail(string sBody)
        {
            var mailMessage = new MimeMessage();
            mailMessage.From.Add(new MailboxAddress(Environment.GetEnvironmentVariable("fromName"), Environment.GetEnvironmentVariable("fromAddress")));
            mailMessage.To.Add(new MailboxAddress(Environment.GetEnvironmentVariable("toName"), Environment.GetEnvironmentVariable("toddress")));
            mailMessage.Subject = "Zero File from Linking Database";
            mailMessage.Body = new TextPart("plain")
            {
                Text = sBody
            };

            using (var smtpClient = new SmtpClient())
            {
                smtpClient.Connect(Environment.GetEnvironmentVariable("smtpServer"), 587, true);
              //  smtpClient.Authenticate("user", "password");
                smtpClient.Send(mailMessage);
                smtpClient.Disconnect(true);
            }
        }
        
        private static LookupForIQC GetLookUpDataFromLinkDB(String content, string filename, string[] parts)
        {
            string[] lines = content.Split(Environment.NewLine);

           /* Console.WriteLine("number of lines=" + lines.Length);
            for (int i=0;i<10;i++)
            {
                Console.WriteLine(lines[i]);
            }
            */
            Dictionary<String, String> dictASMLevelALL = new Dictionary<String, String>();
            
            Dictionary<String, String[]> dictALLIQCParts = new Dictionary<String, String[]>();
            

            HashSet<String> hsParts = new HashSet<String>();
            HashSet<String> hsASM = new HashSet<String>();
            foreach (var i in parts)
            {

                hsParts.Add(i);
            }

            var jsonObj = new LookupForIQC();
            jsonObj.location = "K:/LabData/staging/IQC/linkingdb/aggregated";
            jsonObj.originalFilename = filename;

            jsonObj.data = new List<IQCLookupData>();
            //bool start = false;
            foreach (var line in lines)
            {
               //    Console.WriteLine("line="+line);
                if (!line.Equals("") && line.Split(",", StringSplitOptions.None).Length == 5)
                {
                    var items = line.Split(",", StringSplitOptions.None);
                    var AssmeblyUp = items[1].ToUpperInvariant().Trim();
                    //      Console.WriteLine("key=" + items[1]);
                    var SerialNumberUp = items[2].ToUpperInvariant().Trim();
                    //     Console.WriteLine("key up=" + keyUp);
                    var LinkedTypeUp = items[3].ToUpperInvariant().Trim();
                    var LinkedSerialNumberUp = items[4].ToUpperInvariant().Trim();

                    if (AssmeblyUp.Length >= 13)  //All assembly
                    {
                        if (!string.IsNullOrEmpty(LinkedSerialNumberUp))
                        {

                            if (!dictASMLevelALL.ContainsKey(LinkedSerialNumberUp))
                            {
                                dictASMLevelALL.Add(LinkedSerialNumberUp, SerialNumberUp);
                            }
                        }
                    }

                    if (AssmeblyUp.Length >= 13 && AssmeblyUp.Substring(0, 13).Equals("810-00061-000"))  //assembly for ASM numbers
                    {
                        if (!string.IsNullOrEmpty(SerialNumberUp))
                        {
                            hsASM.Add(SerialNumberUp);
                            
                        }
                    }

                    //else if (AssmeblyUp.Length >= 13 && (AssmeblyUp.Substring(0, 13).Equals("820-00538-000") || AssmeblyUp.Substring(0, 13).Equals("820-00537-000") || AssmeblyUp.Substring(0, 13).Equals("820-00550-000")) && (LinkedTypeUp.Length >= 13 && hsParts.Contains(LinkedTypeUp.Substring(0,13))))
                    else if (AssmeblyUp.Length >= 13 && (LinkedTypeUp.Length >= 13 && hsParts.Contains(LinkedTypeUp.Substring(0, 13))))
                    {
                        if (!string.IsNullOrEmpty(LinkedSerialNumberUp))
                        {
                            String[] str = new String[4];
                            str[0] = AssmeblyUp; str[1] = SerialNumberUp; str[2] = LinkedTypeUp; str[3] = LinkedSerialNumberUp;
                            String key = LinkedTypeUp + "|" + LinkedSerialNumberUp;
                            if (!dictALLIQCParts.ContainsKey(key))
                            {
                                dictALLIQCParts.Add(key, str);
                            }
                        }
                    }
                    
                }

            }

            /*
            foreach(var asmno in hsASM)
                Console.WriteLine("ASM=" + asmno);*/

           // Console.WriteLine("size of dictALLIQCParts=" + dictALLIQCParts.Count);
          //  Console.WriteLine("size of dictASMLevelALL=" + dictASMLevelALL.Count);


            foreach (var item in dictALLIQCParts)
            {
              //  Console.WriteLine("dictALLIQCParts linked=" + item.Key);
                String ASMNumber = "START";
                String key = item.Value[1];
                String Lastkey = "";
                int counter = 0;
                while (!string.IsNullOrEmpty(ASMNumber) && !hsASM.Contains(ASMNumber) && !key.Equals(Lastkey)) {
                  //  Console.WriteLine("key=" + key);
                    ASMNumber = getFromDictioary(key, dictASMLevelALL);
                    Lastkey = key;
                    key = ASMNumber;
                  //  Console.WriteLine("value=" + ASMNumber + " ASM?" +hsASM.Contains(ASMNumber));
                    counter++;
                }
                String partno = "";
                String rev = "";

                if (item.Value[2].Length == 13)
                {
                    partno = item.Value[2];
                }
                if (item.Value[2].Length >= 16)
                {
                    partno = item.Value[2].Substring(0, 13);
                    rev = item.Value[2].Substring(14, 2);
                }
                if (hsParts.Contains(partno) && !string.IsNullOrEmpty(ASMNumber) && ASMNumber.StartsWith("ABW"))
                {
                  //  Console.WriteLine("adding to json ASM=" + ASMNumber + " partno=" + partno + " serialNumber=" + item.Value[3]);
                    jsonObj.data.Add(new IQCLookupData
                    {
                        antennaSN = ASMNumber,
                        productSN = partno,
                        revisionNumber = rev,
                        serialNumber = item.Value[3],
                        Description = "IQC"
                    });
                }
            }




            Console.WriteLine("json object length=" + jsonObj.data.Count);

            return jsonObj;
        }
        public static String getFromDictioary(String key, Dictionary<String,String> dic)
        {
            if (!string.IsNullOrEmpty(key))
            {
                if (dic.ContainsKey(key))
                {
                   // Console.WriteLine("getFromDictioary key=" + key + " value="+ dic[key]);
                    return dic[key];
                }
                else
                    return null;
            }
            else return null;

        }

        public static String[] getFromDictioary2(String key, Dictionary<String, String[]> dic)
        {
            if (!string.IsNullOrEmpty(key))
            {
                if (dic.ContainsKey(key))
                {
                  //  Console.WriteLine("getFromDictioary key=" + key + " value 0=" + dic[key][0] + " 1="+ dic[key][1] + " 2=" + dic[key][2]);
                    return dic[key];
                }
                else
                    return null;
            }
            else return null;

        }

    }

   

    public static class Function3
    {
        [FunctionName("IQCDataSheet")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("BigDataStorage"));
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("W2sakcsStorage"));
                CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawIQCDataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("IQCDataContainer"));

                string datetime = req.Query["days"];

                string sheaderNOM = req.Headers["days"];


                String datetimeDiff = datetime ?? sheaderNOM ?? "30";

                int dateDiffinNumber = 30;
                try
                {
                    dateDiffinNumber = Int32.Parse(datetimeDiff);
                    Console.WriteLine(dateDiffinNumber);
                }
                catch (FormatException)
                {
                    Console.WriteLine($"Unable to parse number days entered - '{dateDiffinNumber}'");
                }

                DateTime datecutoff = DateTime.Now.AddDays(-dateDiffinNumber);


                log.LogInformation("date time cut off to process IQC data = " + datecutoff);

                BlobContinuationToken blobContinuationToken = null;
                int filecounter = 0;
                do
                {
                    var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                        //prefix: "IQC/EV2",
                        prefix: "IQC",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: blobContinuationToken,
                        options: null,
                        operationContext: null
                    );
                    
                    

                    var iqcblobs = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".xls")) && !Path.GetDirectoryName(b.Uri.AbsolutePath).Contains("OLD") && !Path.GetDirectoryName(b.Uri.AbsolutePath).Contains("Test") && (Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("IQC")));
                    log.LogInformation("blob count total for IQC Datasheet files=" + iqcblobs.Count());

                    foreach (var blob in iqcblobs)
                    {
                        var block = blob as CloudBlockBlob;
                        var last_mod = block.Properties.LastModified.Value.DateTime;
                        if (last_mod >= datecutoff) {

                            using (var ms = new MemoryStream())
                            {

                                await block.DownloadToStreamAsync(ms);

                                if (ms == null)
                                {
                                    throw new Exception("the content is null");
                                }
                                var blobPath = block.Name;

                                var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                                var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                                var sDestination = "U8/IQC/Datasheet/" + block.Name.Replace(".xls", ".json");
                                //log.LogInformation("Original abs file path=" + blobPath);
                                //log.LogInformation("destination file path=" + sDestination);
                                var jsonObj = GetFileContent(ms, filename);

                                CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                                var js = JsonConvert.SerializeObject(jsonObj);
                                await cloudBlockBlob.UploadTextAsync(js);
                                filecounter++;
                            }
                        }
                    }

                } while (blobContinuationToken != null); // Loop while the continuation token is not null.

                log.LogInformation("Number of IQC Datasheet files processed =" + filecounter);

                return new OkResult();

            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        
        public static DatasheetIQCChecklist GetFileContent(Stream fileStream, string filename)
        {
            var jsonObj = new DatasheetIQCChecklist();
            jsonObj.originalFilename = filename;
            HSSFWorkbook workbook = new HSSFWorkbook(fileStream);
            //IWorkbook workbook = WorkbookFactory.Create(fileStream);

            IFormulaEvaluator evaluator = workbook.GetCreationHelper().CreateFormulaEvaluator(); //.getCreationHelper().createFormulaEvaluator();
            ISheet reportSheet = workbook.GetSheet("Datasheet");

            if (reportSheet != null)
            {
                int rowCount = reportSheet.LastRowNum + 1;
                int maxSampleNumber = 0;
                int maxSampleTested = 0;
                String[] SerialNumber = new String[50];

                for (int i = 0; i < rowCount; i++)
                {
                    IRow row = reportSheet.GetRow(i);
                    //Console.WriteLine("row number= " + i);

                    if (row != null)
                    {
                        if(i == 2)
                        {
                            if(row.Cells.Count >14)
                            {
                                jsonObj.supplierName = row.Cells[3].GetFormattedCellValue(evaluator);
                                jsonObj.partNumber = row.Cells[9].GetFormattedCellValue(evaluator);
                                jsonObj.partDescription = row.Cells[14].GetFormattedCellValue(evaluator);
                            }

                        } else if(i == 3)
                        {
                            if (row.Cells.Count > 15)
                            {
                                jsonObj.revisionNumber = row.Cells[15].GetFormattedCellValue(evaluator);
                            }

                        } else if (i == 4)
                        {
                            if (row.Cells.Count > 20)
                            {
                                jsonObj.poOrder = row.Cells[3].GetFormattedCellValue();
                                jsonObj.lotNumber = row.Cells[9].GetFormattedCellValue();
                                jsonObj.quantityReceived = row.Cells[14].GetFormattedCellValue();
                                jsonObj.quantityInspected = row.Cells[18].GetFormattedCellValue();
                                jsonObj.quantityRejected = row.Cells[21].GetFormattedCellValue();
                            }

                        } else if (i == 5)
                        {
                            if (row.Cells.Count > 20)
                            {
                                DateTime dt;
                                CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
                                DateTimeStyles styles = DateTimeStyles.None;
                                DateTime.TryParse(row.Cells[8].GetFormattedCellValue(), culture, styles, out dt);
                                jsonObj.dateReceived = dt;
                                DateTime.TryParse(row.Cells[12].GetFormattedCellValue(), culture, styles, out dt);
                                jsonObj.dateInspected = dt;
                                jsonObj.inspectedBy = row.Cells[16].GetFormattedCellValue();
                            }

                        } else if (i == 11)
                        {
                         //   Console.WriteLine("last cell number = " + row.LastCellNum);
                            maxSampleNumber = row.LastCellNum - 8;
                          //  Console.WriteLine("Max sample number = " + maxSampleNumber);
                            SerialNumber = new String[maxSampleNumber];
                            jsonObj.data = new List<DatasheetIQCChecklistData>();

                        }
                        else if (i >= 12 && row.LastCellNum >= maxSampleNumber && (!string.IsNullOrEmpty(row.Cells[2].GetFormattedCellValue()) || !string.IsNullOrEmpty(row.Cells[3].GetFormattedCellValue())))
                        {
                            
                            
                            DatasheetIQCChecklistData eachBubble = new DatasheetIQCChecklistData();
                            eachBubble.bubbleNumber = "0";
                            eachBubble.measures = new List<Measures>();

                            if (i == 12 && row.Cells[3].GetFormattedCellValue().StartsWith("Serial Number"))
                            {

                                
                                int counter = 0;
                                foreach (var cell in row.Cells)
                                {
                                    
                                    var cellValue = cell.GetFormattedCellValue();
                                   // Console.WriteLine("cell index = " + cell.ColumnIndex + "Type=" + cell.CellType);

                                    if (!string.IsNullOrEmpty(cellValue))
                                    {
                                       // Console.WriteLine("cell value = " + cellValue);

                                        if (cell.ColumnIndex >= 8)
                                        {
                                            SerialNumber[counter++] = cellValue;
                                        }


                                    }

                                }
                            }
                            else
                            {
                                int counter = 0;
                                foreach (var cell in row.Cells)
                                {
                                    Measures eachMeasure = new Measures();
                                    var cellValue = cell.GetFormattedCellValue();
                                  //  Console.WriteLine("cell index = " + cell.ColumnIndex + "Type=" + cell.CellType);

                                    if (!string.IsNullOrEmpty(cellValue))
                                    {
                                     //   Console.WriteLine("cell value = " + cellValue);

                                        if (cell.ColumnIndex == 2)
                                            eachBubble.bubbleNumber = cellValue;
                                        else if (cell.ColumnIndex == 3)
                                            eachBubble.location = cellValue;
                                        else if (cell.ColumnIndex == 4)
                                            eachBubble.target = cellValue;
                                        else if (cell.ColumnIndex == 5)
                                            eachBubble.pulsTol = cellValue;
                                        else if (cell.ColumnIndex == 6)
                                            eachBubble.minusTol = cellValue;
                                        else if (cell.ColumnIndex == 7)
                                            eachBubble.inspectionMethod = cellValue;
                                        else
                                        {
                                            eachMeasure.measures = cellValue;
                                            if (counter < SerialNumber.Length) {
                                                eachMeasure.serialNumber = SerialNumber[counter++];
                                            }
                                            eachBubble.measures.Add(eachMeasure);
                                        }


                                    }
                                }


                                jsonObj.data.Add(eachBubble);
                            }

                            

                        }
                        
                        
                    }
                }
            }

            return jsonObj;
        }


        


public static string GetFormattedCellValue(this ICell cell, IFormulaEvaluator eval = null)
        {
            if (cell != null)
            {
                switch (cell.CellType)
                {
                    case CellType.String:
                        return cell.StringCellValue;

                    case CellType.Numeric:
                        if (DateUtil.IsCellDateFormatted(cell))
                        {
                            try
                            {
                                return cell.DateCellValue.ToString();
                            }
                            catch (NullReferenceException)
                            {
                                return DateTime.FromOADate(cell.NumericCellValue).ToString();
                            }
                        }
                        return cell.NumericCellValue.ToString();


                    case CellType.Boolean:
                        return cell.BooleanCellValue ? "TRUE" : "FALSE";

                    case CellType.Formula:                        
                        if (eval != null)
                        {
                            
                            return GetFormattedCellValue(eval.EvaluateInCell(cell));
                        }
                        else
                        {
                            
                            return cell.CellFormula;
                        }

                    case CellType.Error:
                        return FormulaError.ForInt(cell.ErrorCellValue).String;
                }
            }
            // null or blank cell, or unknown cell type
            return string.Empty;
        }




    }

}

