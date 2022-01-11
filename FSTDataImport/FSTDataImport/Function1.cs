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

namespace FSTDataImport
{
    public static class Function1
    {
        [FunctionName("CopyFSTData")]
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

                string Numofmonth = req.Query["numberOfMonth"];

                string sheaderNOM = req.Headers["numberOfMonth"];


                Numofmonth = Numofmonth ?? sheaderNOM;

                var NumofmonthInt = -1;
                try
                {
                    NumofmonthInt = -1 * int.Parse(Numofmonth);
                }
                catch
                {
                    NumofmonthInt = -1;
                }
                log.LogInformation("number of month = " + NumofmonthInt);

                var blobs_crosswalk = await container.ListBlobsSegmentedAsync("ASMSN_Crosswalk", true, BlobListingDetails.All, null, null, null, null);
                Dictionary<string, string> SegmentCW = new Dictionary<string, string>();
                foreach (var blob in blobs_crosswalk.Results)
                {

                    var block = blob as CloudBlockBlob;

                    var content = await block.DownloadTextAsync();

                    // Var filename = blob.Uri.
                    //log.LogInformation("Original abs file path=" + blob.Uri.AbsolutePath);

                    SegmentCW = GetASMFSTCrosswalk(content);

                }

                var blobs = await rawContainer.ListBlobsSegmentedAsync("", true, BlobListingDetails.All, null, null, null, null);
                foreach (var blob in blobs.Results)
                {

                    var block = blob as CloudBlockBlob;

                    var content = await block.DownloadTextAsync();

                    // Var filename = blob.Uri.
                    //log.LogInformation("Original abs file path=" + blob.Uri.AbsolutePath);

                    var blobPath = blob.Uri.AbsolutePath.Substring(blob.Uri.AbsolutePath.IndexOf('/', 1) + 1);
                    var blobpatharray = blobPath.Split("/");
                    var sdatepreviousday = DateTime.Now.AddMonths(NumofmonthInt);
                    var sDestination = "";
                    if (blobpatharray.Length == 4)
                    {
                        DateTime oDate = DateTime.ParseExact(blobpatharray[1], "yyyy-MM", null);
                        //log.LogInformation("oDate=" + oDate);
                        //log.LogInformation("sdatepreviousday=" + sdatepreviousday);
                        if (blobpatharray[3].EndsWith(".csv") && (oDate.CompareTo(sdatepreviousday) >= 0)) //summary file in the date range
                        {
                            sDestination = blobpatharray[0] + "/" + blobpatharray[1].Substring(0, 4) + "/" + blobpatharray[1].Substring(5) + "/" + "summary" + "/" + blobpatharray[3].Replace(".csv", ".json");
                            var jsonObj = GetASMFSTSummaryData(content, blobpatharray[3], SegmentCW);
                            log.LogInformation("summary file path=" + blobPath);
                            log.LogInformation("destination file path=" + sDestination);
                            CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                            var js = JsonConvert.SerializeObject(jsonObj);
                            await cloudBlockBlob.UploadTextAsync(js);

                        }
                        //else if (blobpatharray[3].Contains(".s2p") && (oDate.CompareTo(sdatepreviousday) >= 0)) //s2p file
                        //{
                        //sDestination = blobpatharray[0] + "/" + blobpatharray[1].Substring(0, 4) + "/" + blobpatharray[1].Substring(5) + "/" + "s2p" + "/" + blobpatharray[2].Replace(".s2p", ".json");
                        // var jsonObj = GetS2PCSV(content, blobpatharray[3], oDate.ToString(), blobpatharray[1].Trim());
                        // log.LogInformation("s2p file path=" + blobPath);
                        // CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                        // var js = JsonConvert.SerializeObject(jsonObj);
                        // await cloudBlockBlob.UploadTextAsync(js);
                        // }
                    }



                }
                return new OkResult();
                //return (ActionResult)new OkObjectResult("{\"Id\":\"123\"}");
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        private static FSTSummary GetASMFSTSummaryData(String content, String filename, Dictionary<string, string> dic_cw)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new FSTSummary();
            string segmentSN = lines.First(l => l.StartsWith("Segment SN")).Substring(lines.First(l => l.StartsWith("Segment SN")).IndexOf(',') + 1).Trim();
            string ASMSN = "";
            dic_cw.TryGetValue(segmentSN, out ASMSN);
            
            jsonObj.OriginalFilename = filename.Trim();
            jsonObj.SoftwareName = lines.First(l => l.StartsWith("Software Name")).Substring(lines.First(l => l.StartsWith("Software Name")).IndexOf(',') + 1).Trim();
            jsonObj.SoftwareVersion = lines.First(l => l.StartsWith("Software Version")).Substring(lines.First(l => l.StartsWith("Software Version")).IndexOf(',') + 1).Trim();
            jsonObj.DTG = lines.First(l => l.StartsWith("DTG")).Substring(lines.First(l => l.StartsWith("DTG")).IndexOf(',') + 1).Trim();
            jsonObj.SegmentPN = lines.First(l => l.StartsWith("Segment PN")).Substring(lines.First(l => l.StartsWith("Segment PN")).IndexOf(',') + 1).Trim();
            jsonObj.Operator = lines.First(l => l.StartsWith("Operator")).Substring(lines.First(l => l.StartsWith("Operator")).IndexOf(',') + 1).Trim();
            jsonObj.SegmentSN = segmentSN;
            jsonObj.ASMSN = ASMSN;
            jsonObj.Filename = lines.First(l => l.StartsWith("FileName")).Substring(lines.First(l => l.StartsWith("FileName")).IndexOf(',') + 1).Trim();
            jsonObj.Location = lines.First(l => l.StartsWith("Location")).Substring(lines.First(l => l.StartsWith("Location")).IndexOf(',') + 1).Trim();
            jsonObj.VNASettings = (lines.First(l => l.StartsWith("VNA Settings")).Substring(lines.First(l => l.StartsWith("VNA Settings")).IndexOf(',') + 1).Trim()).Replace("\"[", "").Replace("]\"", "").Split(",").Select(t => t.Trim()).ToList();
            jsonObj.VNACalibration = lines.First(l => l.StartsWith("VNA Calibration")).Substring(lines.First(l => l.StartsWith("VNA Calibration")).IndexOf(',') + 1).Trim();
            jsonObj.ControllerInformation = lines.First(l => l.StartsWith("Controller")).Substring(lines.First(l => l.StartsWith("Controller")).IndexOf(',') + 1).Trim();
            jsonObj.ConfigFileVersion = lines.First(l => l.StartsWith("Config")).Substring(lines.First(l => l.StartsWith("Config")).IndexOf(',') + 1).Trim();

            jsonObj.data = new List<FSTSummaryData>();

            var newVersion = false;
            if (jsonObj.SoftwareVersion.CompareTo("1.0.0") >= 0) newVersion = true;

            Console.WriteLine("software version=" + newVersion);

            bool start = false;
            foreach (var item in lines)
            {
                if (start)
                {
                    var line = item;
                    if (!newVersion)
                    {
                        line = line + ",,0.0,0.0";
                    }
                    var items = line.Split(",", StringSplitOptions.None);
                    if (items.Length == 18)
                    {
                        try
                        {
                            jsonObj.data.Add(new FSTSummaryData
                            {
                                RxStatus = items[0],
                                TxStatus = items[1],
                                DACCount = items[2],
                                RxCFGHz = double.Parse(items[3]),
                                RxDB = double.Parse(items[4]),
                                TxCFGHz = double.Parse(items[5]),
                                TxDB = double.Parse(items[6]),
                                NullCFGHz = double.Parse(items[7]),
                                NullDB = double.Parse(items[8]),
                                SourceCurrent = double.Parse(items[9]),
                                Current = double.Parse(items[10]),
                                TemperatureInC = double.Parse(items[11]),
                                Humidity = double.Parse(items[12]),
                                DateTime = DateTime.ParseExact(items[13], "yyyyMMddHHmmss", null),
                                S2pFile = items[14],
                                Rx2Status = items[15],
                                Rx2CFGHz = double.Parse(items[16]),
                                Rx2DB = double.Parse(items[17])
                            });
                        }
                        catch (Exception e)
                        {
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

            }

            return jsonObj;
        }

        private static Dictionary<string, string> GetASMFSTCrosswalk(String content)
        {
            string[] lines = content.Split(Environment.NewLine);

            Dictionary<string, string> d = new Dictionary<string, string>();

            foreach (var item in lines)
            {
                var items = item.Split(",", StringSplitOptions.RemoveEmptyEntries);
                if (items.Length == 5)
                {
                    d.Add(items[1], items[0]);
                    d.Add(items[2], items[0]);
                    d.Add(items[3], items[0]);
                    d.Add(items[4], items[0]);
                }
            }
            return d;
        }
    }
}
