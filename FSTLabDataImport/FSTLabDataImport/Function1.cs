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
using System.Collections.Generic;

namespace FSTLabDataImport
{
    public static class Function1
    {
        [FunctionName("CopyFSTLabDataData")]
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
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawFSTDataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("FSTDataContainer"));

                string datetime = req.Query["yearAndMonth"];

                string sheaderNOM = req.Headers["yearAndMonth"];


                datetime = datetime ?? sheaderNOM ?? DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString();


                log.LogInformation("date time = " + datetime);

                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                        prefix: "IQC/DV",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: blobContinuationToken,
                        options: null,
                        operationContext: null
                    );

                    // load the lookup csv to load the lookup file
                    var blobForLookup = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv")) && (Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).Contains("Lookup")));
                    log.LogInformation("blob count total for lookup files=" + blobForLookup.Count());

                    foreach (var blob in blobForLookup)
                    {
                        var block = blob as CloudBlockBlob;

                        var content = await block.DownloadTextAsync();

                        var blobPath = block.Name;
                        var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                        var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                        var sDestination = "DV/LookupData/" + block.Name.Replace(".csv", ".json");
                        log.LogInformation("Original abs file path=" + blobPath);
                        log.LogInformation("destination file path=" + sDestination);
                        var jsonObj = GetLookUpData(content, filename, filepath);

                        CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                        var js = JsonConvert.SerializeObject(jsonObj);
                        await cloudBlockBlob.UploadTextAsync(js);
                    }

                    // Get the value of the continuation token returned by the listing call.
                    blobContinuationToken = resultSegment.ContinuationToken;
                    var blobs = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv") && !Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).Contains("Lookup")));
                    log.LogInformation("blob count total for FST files=" + blobs.Count());

                    foreach (var blob in blobs)
                    {
                        var block = blob as CloudBlockBlob;

                        var content = await block.DownloadTextAsync();

                        var blobPath = block.Name;
                        var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                        var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                        var sDestination = "DV/FST/" + block.Name.Replace(".csv", ".json");
                        //log.LogInformation("Original abs file path=" + blobPath);
                        //log.LogInformation("destination file path=" + sDestination);
                        var jsonObj = GetASMFSTSummaryData(content, filename, filepath);

                        CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                        var js = JsonConvert.SerializeObject(jsonObj);
                        await cloudBlockBlob.UploadTextAsync(js);
                    }
                } while (blobContinuationToken != null); // Loop while the continuation token is not null.

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

        private static FSTGenericSummary GetASMFSTSummaryData(String content, String filename, String filepath)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new FSTGenericSummary();
            string segmentSN = lines.First(l => l.StartsWith("Segment SN")).Substring(lines.First(l => l.StartsWith("Segment SN")).IndexOf(',') + 1).Trim();


            jsonObj.OriginalFilename = filename.Trim();
            jsonObj.SoftwareName = lines.First(l => l.StartsWith("Software Name")).Substring(lines.First(l => l.StartsWith("Software Name")).IndexOf(',') + 1).Trim();
            jsonObj.SoftwareVersion = lines.First(l => l.StartsWith("Software Version")).Substring(lines.First(l => l.StartsWith("Software Version")).IndexOf(',') + 1).Trim();
            try
            {
                jsonObj.DTG = DateTime.ParseExact(lines.First(l => l.StartsWith("DTG")).Substring(lines.First(l => l.StartsWith("DTG")).IndexOf(',') + 1).Trim(), "yyyyMMddHHmmss", null);
            }
            catch (Exception e)
            {
                Console.WriteLine("exception for file " + filepath);
                Console.WriteLine("exception=" + e.StackTrace);
            }

            jsonObj.SegmentPN = lines.First(l => l.StartsWith("Segment PN")).Substring(lines.First(l => l.StartsWith("Segment PN")).IndexOf(',') + 1).Trim();

            jsonObj.Operator = lines.First(l => l.StartsWith("Operator")).Substring(lines.First(l => l.StartsWith("Operator")).IndexOf(',') + 1).Trim();
            jsonObj.SegmentSN = segmentSN;
            jsonObj.Filename = lines.First(l => l.StartsWith("FileName")).Substring(lines.First(l => l.StartsWith("FileName")).IndexOf(',') + 1).Trim();
            jsonObj.Location = lines.First(l => l.StartsWith("Location")).Substring(lines.First(l => l.StartsWith("Location")).IndexOf(',') + 1).Trim();
            try
            {
                jsonObj.VNASettings = (lines.First(l => l.StartsWith("VNA Settings")).Substring(lines.First(l => l.StartsWith("VNA Settings")).IndexOf(',') + 1).Trim()).Replace("\"[", "").Replace("]\"", "").Split(",").Select(t => t.Trim()).ToList();
                jsonObj.VNACalibration = lines.First(l => l.StartsWith("VNA Calibration")).Substring(lines.First(l => l.StartsWith("VNA Calibration")).IndexOf(',') + 1).Trim();
                jsonObj.ControllerInformation = lines.First(l => l.StartsWith("Controller")).Substring(lines.First(l => l.StartsWith("Controller")).IndexOf(',') + 1).Trim();
                jsonObj.ConfigFileVersion = lines.First(l => l.StartsWith("Config")).Substring(lines.First(l => l.StartsWith("Config")).IndexOf(',') + 1).Trim();
            }
            catch (Exception e)
            {
                //
            }

            try
            {
                var newSegmentPN = lines.First(l => l.StartsWith("New Segment PN")).Substring(lines.First(l => l.StartsWith("New Segment PN")).IndexOf(',') + 1).Trim();
                if (!newSegmentPN.Equals("")) jsonObj.SegmentPN = newSegmentPN;
            }
            catch (Exception e)
            {
                //
            }

            jsonObj.data = new List<FSTGenericSummaryData>();

            var newVersion = false;
            if (jsonObj.SoftwareVersion.CompareTo("1.0.0") >= 0) newVersion = true;

            //Console.WriteLine("software version=" + newVersion);

            bool start = false;
            foreach (var item in lines)
            {
                if (start)
                {
                    var line = item;
                    //if (!newVersion)
                    //{
                    //    line = line + ",,0.0,0.0";
                    //}
                    var items = line.Split(",", StringSplitOptions.None);
                    if (items.Length == 20)
                    {
                        try
                        {
                            jsonObj.data.Add(new FSTGenericSummaryData
                            {
                                RxStatus = items[0],
                                TxStatus = items[1],
                                DACCount = items[2],
                                RxCFGHz = double.Parse(items[3].Equals("") ? "0.0" : items[3]),
                                RxDB = double.Parse(items[4].Equals("") ? "0.0" : items[4]),
                                TxCFGHz = double.Parse(items[5].Equals("") ? "0.0" : items[5]),
                                TxDB = double.Parse(items[6].Equals("") ? "0.0" : items[6]),
                                NullCFGHz = double.Parse(items[7].Equals("") ? "0.0" : items[7]),
                                NullDB = double.Parse(items[8].Equals("") ? "0.0" : items[8]),
                                SourceCurrent = double.Parse(items[9].Equals("") ? "0.0" : items[9]),
                                Current = double.Parse(items[10].Equals("") ? "0.0" : items[10]),
                                TemperatureInC = double.Parse(items[11].Equals("") ? "0.0" : items[11]),
                                Humidity = double.Parse(items[12].Equals("") ? "0.0" : items[12]),
                                DateTime = DateTime.ParseExact(items[13], "yyyyMMddHHmmss", null),
                                S2pFile = items[14],
                                GateCurrent = double.Parse(items[15].Equals("") ? "0.0" : items[15]),
                                Rx2Status = items[16],
                                Rx2CFGHz = double.Parse(items[17].Equals("") ? "0.0" : items[17]),
                                Rx2DB = double.Parse(items[18].Equals("") ? "0.0" : items[18]),
                                Q1TempC = double.Parse(items[19].Equals("") ? "0.0" : items[19])
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
                if (item.StartsWith("Rx Status"))
                {
                    start = true;
                }
                if (item.StartsWith("RX_Tuning_Range"))
                {
                    start = false;
                }
                if (item.StartsWith("Rx1 Resonance"))
                {
                    start = false;
                }

            }

            return jsonObj;
        }
    }
}
