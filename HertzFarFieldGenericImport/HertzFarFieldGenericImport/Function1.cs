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

namespace HertzFarFieldGenericImport
{
    public static class Function1
    {
        [FunctionName("CopyHertzDataGenericImport")]
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
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawHertzDataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("HertzDataContainer"));

                int dateDiffinNumber = 270;

                DateTime datecutoff = DateTime.Now.AddDays(-dateDiffinNumber);


                log.LogInformation("date time cut off to process Hertz data = " + datecutoff);

                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                        prefix: "hertz",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: blobContinuationToken,
                        options: null,
                        operationContext: null
                    );

                    // Get the value of the continuation token returned by the listing call.
                    blobContinuationToken = resultSegment.ContinuationToken;
                    var blobs = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv") && Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).Contains("Firstpass_Report")));
                    log.LogInformation("blob count total for Hertz files=" + blobs.Count());

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
                            var sDestination = "Post_OptS11_Hertz/ATPSummary/" + block.Name.Replace(".csv", ".json");
                           // var sDestination = "EV2/Post_OptS11_Hertz/ATPSummary/" + block.Name.Replace(".csv", ".json");
                            //log.LogInformation("Original abs file path=" + blobPath);
                            //log.LogInformation("destination file path=" + sDestination);
                            var jsonObj = GetHertzFarFieldSummaryData(content, filename, filepath);

                            CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                            var js = JsonConvert.SerializeObject(jsonObj);
                            await cloudBlockBlob.UploadTextAsync(js);
                        }
                    }
                } while (blobContinuationToken != null); // Loop while the continuation token is not null.
   
                return new OkResult();
                //return (ActionResult)new OkObjectResult("{\"Id\":\"123\"}");
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        private static HertzFarFieldSummary GetHertzFarFieldSummaryData(String content, string filename, string filelocation)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new HertzFarFieldSummary();

            jsonObj.OriginalFilename = filename.Trim();

            jsonObj.FileLocation = filelocation.Trim();

            var filenameArray = filename.Split("_", StringSplitOptions.None);

            jsonObj.data = new List<HertzFarFieldSummaryData>();


            foreach (var item in lines)
            {
                if (!item.StartsWith(","))
                {
                    var items = item.Split(",", StringSplitOptions.None);
                    if (items.Length == 18
                        )
                    {
                        try
                        {
                            if (items[0].Equals("0"))
                            {
                                var serialnum = items[1].Split(" ", StringSplitOptions.None);
                                //jsonObj.ASMSN = serialnum[0].Equals("") ? filenameArray[0] : serialnum[0].Trim();

                                if (serialnum.Length > 0)
                                {
                                    jsonObj.ASMSN = serialnum[0].Equals("") ? filenameArray[0] : serialnum[0].Trim();
                                } else
                                {
                                    jsonObj.ASMSN = filenameArray[0];
                                }
                                //Console.WriteLine("ASM NO=" + jsonObj.ASMSN);

                                if (serialnum.Length > 1)
                                {
                                    jsonObj.ASMModelNumber = serialnum[1].Trim();
                                }
                                
                                DateTime dt;
                                CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
                                DateTimeStyles styles = DateTimeStyles.None;
                                DateTime.TryParse(items[2].Trim(), culture, styles, out dt);
                                jsonObj.TestDateTime = dt;
                            }
                            jsonObj.data.Add(new HertzFarFieldSummaryData
                            {
                                ScanNumber = int.Parse(items[0].Trim()),
                                Frequency = double.Parse(items[3].Equals("") ? "0.0" : items[3]),
                                GaindBi = double.Parse(items[4].Equals("") ? "0.0" : items[4]),
                                Xpd = double.Parse(items[5].Equals("inf") ? "999999" : (items[5].Equals("") ? "0.0" : items[5])),
                                Sidelobe2DBc = double.Parse(items[6].Equals("") ? "0.0" : items[6]),
                                IbwHigherGhz = double.Parse(items[7].Equals("") ? "0.0" : items[7]),
                                Sidelobe1DBc = double.Parse(items[8].Equals("") ? "0.0" : items[8]),
                                IbwLowerGhz = double.Parse(items[9].Equals("") ? "0.0" : items[9]),
                                FirstdbIbw = double.Parse(items[10].Equals("") ? "0.0" : items[10]),
                                PatternLPA = double.Parse(items[11].Equals("") ? "0.0" : items[11]),
                                PatternPHI = double.Parse(items[12].Equals("") ? "0.0" : items[12]),
                                PatternTHETA = double.Parse(items[13].Equals("") ? "0.0" : items[13]),
                                FrequenciesGHz = double.Parse(items[14].Equals("") ? "0.0" : items[14]),
                                OptimizedS21LPA90P0T0 = items[15].Equals("") ? "0.0" : items[15],
                                TheoreticalDirectivity = double.Parse(items[16].Equals("") ? "0.0" : items[16]),
                                NF2FF = items[17]
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception=" + e.StackTrace);

                            continue;
                        }
                    }
                    else if (items.Length >= 20
                        )
                    {
                        try
                        {
                            if (items[0].Equals("0"))
                            {
                                var serialnum = items[1].Split(" ", StringSplitOptions.None);
                                if (serialnum.Length > 0)
                                {
                                    jsonObj.ASMSN = serialnum[0].Equals("") ? filenameArray[0] : serialnum[0].Trim();
                                }
                                else
                                {
                                    jsonObj.ASMSN = filenameArray[0];
                                }
                                Console.WriteLine("ASM NO=" + jsonObj.ASMSN);
                                
                                if (serialnum.Length > 1)
                                {
                                    jsonObj.ASMModelNumber = serialnum[1].Trim();
                                }
                                DateTime dt;
                                CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
                                DateTimeStyles styles = DateTimeStyles.None;
                                String sDateTime = items[2].Equals("") ? items[3] : items[2];
                                DateTime.TryParse(sDateTime.Trim(), culture, styles, out dt);
                                jsonObj.TestDateTime = dt;
                            }
                            jsonObj.data.Add(new HertzFarFieldSummaryData
                            {
                                ScanNumber = int.Parse(items[0].Trim()),
                                Frequency = double.Parse(items[4].Equals("") ? "0.0" : items[4]),
                                GaindBi = double.Parse(items[5].Equals("") ? "0.0" : items[5]),
                                Xpd = double.Parse(items[6].Equals("inf") ? "999999" : (items[6].Equals("") ? "0.0" : items[6])),
                                Sidelobe2DBc = double.Parse(items[7].Equals("") ? "0.0" : items[7]),
                                IbwHigherGhz = double.Parse(items[8].Equals("") ? "0.0" : items[8]),
                                Sidelobe1DBc = double.Parse(items[9].Equals("") ? "0.0" : items[9]),
                                IbwLowerGhz = double.Parse(items[10].Equals("") ? "0.0" : items[10]),
                                FirstdbIbw = double.Parse(items[11].Equals("") ? "0.0" : items[11]),
                                PatternLPA = double.Parse(items[12].Equals("") ? "0.0" : items[12]),
                                PatternPHI = double.Parse(items[13].Equals("") ? "0.0" : items[13]),
                                PatternTHETA = double.Parse(items[14].Equals("") ? "0.0" : items[14]),
                                FrequenciesGHz = double.Parse(items[16].Equals("") ? "0.0" : items[16]),
                                OptimizedS21LPA90P0T0 = items[17].Equals("") ? "0.0" : items[17],
                                TheoreticalDirectivity = double.Parse(items[18].Equals("") ? "0.0" : items[18]),
                                NF2FF = items[19],
                                RXGainFlatnessDb125MHz = double.Parse(items[20].Equals("") ? "0.0" : items[20]),
                                RXGainFlatnessDb230_4MHz = double.Parse(items[21].Equals("") ? "0.0" : items[21]),
                                TXGainFlatnessDb62_5MHz = double.Parse(items[22].Equals("") ? "0.0" : items[22]),
                                TXGainFlatnessDb20MHz = double.Parse(items[23].Equals("") ? "0.0" : items[23]),
                                TXGainFlatnessDb20MHzLow = double.Parse(items[24].Equals("") ? "0.0" : items[24]),
                                TXGainFlatnessDb20MHzHigh = double.Parse(items[25].Equals("") ? "0.0" : items[25])
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("filepath=" + filelocation);
                            Console.WriteLine("filename=" + filename);
                            Console.WriteLine("exception=" + e.StackTrace);

                            continue;
                        }
                    }
                }

            }

            return jsonObj;
        }
    }

    public static class Function2
    {
        [FunctionName("CopyHertzDataBulkImport")]
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
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawHertzDataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("HertzDataContainer"));


                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                        prefix: "ASM_Test4_Hertz/Bulk-ABC",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: blobContinuationToken,
                        options: null,
                        operationContext: null
                    );

                    // Get the value of the continuation token returned by the listing call.
                    blobContinuationToken = resultSegment.ContinuationToken;
                    var blobs = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv")));
                    log.LogInformation("blob count total for Hertz files=" + blobs.Count());

                    foreach (var blob in blobs)
                    {
                        var block = blob as CloudBlockBlob;

                        var content = await block.DownloadTextAsync();

                        var blobPath = block.Name;
                        var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                        var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));                        
                        log.LogInformation("Original abs file path=" + blobPath);
                        await GetHertzFarFieldSummaryData(content, filename, filepath, container);                        
                    }
                } while (blobContinuationToken != null); // Loop while the continuation token is not null.

                return new OkResult();
                //return (ActionResult)new OkObjectResult("{\"Id\":\"123\"}");
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        private static async Task<IActionResult> GetHertzFarFieldSummaryData(String content, string filename, string filelocation, CloudBlobContainer container)
        {
            string[] lines = content.Split(Environment.NewLine);

            HertzFarFieldSummary jsonObj = null;
            string fname_dest = null;
            string AsmNumber = "";
            string PreviousAsm = "";
                foreach (var item in lines)
                {
                    if (!item.StartsWith(","))
                    {
                        var items = item.Split(",", StringSplitOptions.None);
                        if (items.Length == 18
                            )
                        {
                            try
                            {
                            PreviousAsm = AsmNumber;
                            var serialnum = items[1].Split(" ", StringSplitOptions.None);
                            AsmNumber = serialnum[0].Trim();
                            
                            if (!AsmNumber.Equals(PreviousAsm))
                                {
                                    if (jsonObj != null)
                                    {
                                        var sDestination = "Post_OptS11_Hertz/ATPSummary/" + filelocation + "/" + fname_dest.Replace("/","_");
                                        CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                                        var js = JsonConvert.SerializeObject(jsonObj);
                                        await cloudBlockBlob.UploadTextAsync(js);
                                        //Console.WriteLine(fname_dest);
                                }

                                    jsonObj = new HertzFarFieldSummary();
                                    
                                    jsonObj.ASMSN = AsmNumber;
                                    
                                    DateTime dt;
                                    CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
                                    DateTimeStyles styles = DateTimeStyles.None;
                                    DateTime.TryParse(items[2].Trim(), culture, styles, out dt);
                                    jsonObj.TestDateTime = dt;
                                    jsonObj.OriginalFilename = filename.Trim();
                                    jsonObj.FileLocation = filelocation.Trim();
                                    fname_dest = jsonObj.ASMSN + "_" + jsonObj.TestDateTime.Ticks + ".json";
                                    if (serialnum.Length > 1)
                                    {
                                        jsonObj.ASMModelNumber = serialnum[1].Trim();
                                        
                                    }
                                    jsonObj.data = new List<HertzFarFieldSummaryData>();
                                    //Console.WriteLine(jsonObj.ASMSN);
                            }
                                jsonObj.data.Add(new HertzFarFieldSummaryData
                                {
                                    ScanNumber = int.Parse(items[0].Trim()),
                                    Frequency = double.Parse(items[3].Equals("") ? "0.0" : items[3]),
                                    GaindBi = double.Parse(items[4].Equals("") ? "0.0" : items[4]),
                                    Xpd = double.Parse(items[5].Equals("inf") ? "999999" : (items[5].Equals("") ? "0.0" : items[5])),
                                    Sidelobe2DBc = double.Parse(items[6].Equals("") ? "0.0" : items[6]),
                                    IbwHigherGhz = double.Parse(items[7].Equals("") ? "0.0" : items[7]),
                                    Sidelobe1DBc = double.Parse(items[8].Equals("") ? "0.0" : items[8]),
                                    IbwLowerGhz = double.Parse(items[9].Equals("") ? "0.0" : items[9]),
                                    FirstdbIbw = double.Parse(items[10].Equals("") ? "0.0" : items[10]),
                                    PatternLPA = double.Parse(items[11].Equals("") ? "0.0" : items[11]),
                                    PatternPHI = double.Parse(items[12].Equals("") ? "0.0" : items[12]),
                                    PatternTHETA = double.Parse(items[13].Equals("") ? "0.0" : items[13]),
                                    FrequenciesGHz = double.Parse(items[14].Equals("") ? "0.0" : items[14]),
                                    OptimizedS21LPA90P0T0 = items[15].Equals("") ? "0.0" : items[15],
                                    TheoreticalDirectivity = double.Parse(items[16].Equals("") ? "0.0" : items[16]),
                                    NF2FF = items[17]
                                });
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("exception=" + e.StackTrace);

                                continue;
                            }
                        }
                        else if (items.Length == 20
                            )
                        {
                            try
                            {
                                if (items[0].Equals("0"))
                                {
                                    if (jsonObj != null)
                                    {
                                        var sDestination = "Post_OptS11_Hertz/ATPSummary/" + filelocation + "/" + fname_dest;
                                        CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                                        var js = JsonConvert.SerializeObject(jsonObj);
                                        await cloudBlockBlob.UploadTextAsync(js);
                                    }
                                    jsonObj = new HertzFarFieldSummary();
                                    var serialnum = items[1].Split(" ", StringSplitOptions.None);
                                    jsonObj.ASMSN = serialnum[0].Trim();
                                    if (serialnum.Length > 1)
                                    {
                                        jsonObj.ASMModelNumber = serialnum[1].Trim();
                                    }
                                    DateTime dt;
                                    CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
                                    DateTimeStyles styles = DateTimeStyles.None;
                                    String sDateTime = items[2].Equals("") ? items[3] : items[2];
                                    DateTime.TryParse(sDateTime.Trim(), culture, styles, out dt);
                                    jsonObj.TestDateTime = dt;
                                    jsonObj.OriginalFilename = filename.Trim();
                                    jsonObj.FileLocation = filelocation.Trim();
                                    fname_dest = jsonObj.ASMSN + "_" + jsonObj.TestDateTime.Ticks + ".json";
                                    jsonObj.data = new List<HertzFarFieldSummaryData>();
                            }
                                jsonObj.data.Add(new HertzFarFieldSummaryData
                                {
                                    ScanNumber = int.Parse(items[0].Trim()),
                                    Frequency = double.Parse(items[4].Equals("") ? "0.0" : items[4]),
                                    GaindBi = double.Parse(items[5].Equals("") ? "0.0" : items[5]),
                                    Xpd = double.Parse(items[6].Equals("inf") ? "999999" : (items[6].Equals("") ? "0.0" : items[6])),
                                    Sidelobe2DBc = double.Parse(items[7].Equals("") ? "0.0" : items[7]),
                                    IbwHigherGhz = double.Parse(items[8].Equals("") ? "0.0" : items[8]),
                                    Sidelobe1DBc = double.Parse(items[9].Equals("") ? "0.0" : items[9]),
                                    IbwLowerGhz = double.Parse(items[10].Equals("") ? "0.0" : items[10]),
                                    FirstdbIbw = double.Parse(items[11].Equals("") ? "0.0" : items[11]),
                                    PatternLPA = double.Parse(items[12].Equals("") ? "0.0" : items[12]),
                                    PatternPHI = double.Parse(items[13].Equals("") ? "0.0" : items[13]),
                                    PatternTHETA = double.Parse(items[14].Equals("") ? "0.0" : items[14]),
                                    FrequenciesGHz = double.Parse(items[16].Equals("") ? "0.0" : items[16]),
                                    OptimizedS21LPA90P0T0 = items[17].Equals("") ? "0.0" : items[17],
                                    TheoreticalDirectivity = double.Parse(items[18].Equals("") ? "0.0" : items[18]),
                                    NF2FF = items[19]
                                });
                            }
                            catch (Exception e)
                            {
                                Console.WriteLine("exception=" + e.StackTrace);

                                continue;
                            }
                        }
                    }

                }

            return new OkResult();
        }
    }
}
