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

namespace Test2DataImport
{
    public static class Function1
    {
        [FunctionName("Test2DataImportFunction")]
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
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawTest2DataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("Test2DataContainer"));

                string datetime = req.Query["yearAndMonth"];

                string sheaderNOM = req.Headers["yearAndMonth"];


                datetime = datetime ?? sheaderNOM ?? DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString();


                log.LogInformation("date time = " + datetime);


                var list = await rawContainer.ListBlobsSegmentedAsync("ASM_Test2/" + datetime, true, BlobListingDetails.All, null, null, null, null);
                //Loading summary files (.csv)
                var blobs = list.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv")));
                log.LogInformation("blob count for csv files=" + blobs.Count());

                foreach (var blob in blobs)
                {
                    var block = blob as CloudBlockBlob;

                    var blobPath = block.Name;

                    var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    var sDestinationSummary = "Post_OptS11_Test2/Summary/" + block.Name.Replace(".csv", ".json");

                    //log.LogInformation("csv blob destination=" + sDestinationSummary);

                    var content = await block.DownloadTextAsync();
                    var jsonObj = GetTest2Data(content, filename, filepath);

                    CloudBlockBlob cloudBlockBlobSummary = container.GetBlockBlobReference(sDestinationSummary);
                    var js = JsonConvert.SerializeObject(jsonObj);
                    await cloudBlockBlobSummary.UploadTextAsync(js);
                }

                //Loading raw files (.json)
                var jsonblobs = list.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".json")));
                log.LogInformation("blob count for json files=" + jsonblobs.Count());

                foreach (var blob in jsonblobs)
                {
                    var block = blob as CloudBlockBlob;
               
                    var sDestinationRaw = "Post_OptS11_Test2/Raw" + "/" + block.Name;                   
                    CloudBlockBlob cloudBlockBlobRaw = container.GetBlockBlobReference(sDestinationRaw);
                    //log.LogInformation("json blob source=" + block.Name);
                    //log.LogInformation("json blob destination=" + sDestinationRaw);
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

                return new OkObjectResult("");
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }

        }

        private static Test2Summary GetTest2Data(String content, string filename, string filelocation)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = new Test2Summary();

            jsonObj.OriginalFilename = filename.Trim();

            jsonObj.FileLocation = filelocation.Trim();

            try
            {
                jsonObj.AntennaSN = lines.First(l => l.StartsWith("Antenna SN")).Split(",")[1].Trim();
            }
            catch (Exception e)
            {
                Console.WriteLine("AntennaSN blank " + filename);
            }
            try
            {
                jsonObj.AntennaPN = lines.First(l => l.StartsWith("Antenna PN")).Split(",")[1].Trim();
            }
            catch (Exception e)
            {
                Console.WriteLine("AntennaPN blank " + filename);
            }
            try
            {
                jsonObj.FeedSN = lines.First(l => l.StartsWith("Feed SN")).Split(",")[1].Trim();
            }
            catch (Exception e)
            {
                Console.WriteLine("FeedSN blank " + filename);
            }
            try
            {
                jsonObj.KIRUSN = lines.First(l => l.StartsWith("KIRU SN")).Split(",")[1].Trim();
            }
            catch (Exception e)
            {
                Console.WriteLine("KIRUSN blank " + filename);
            }
            try { 
                jsonObj.TRCBSN = lines.First(l => l.StartsWith("TRCB SN")).Split(",")[1].Trim();
            }
            catch (Exception e)
            {
                Console.WriteLine("TRCBSN blank " + filename);
            }

            string strdt = lines.First(l => l.StartsWith("DTG")).Split(",")[1].Trim();
            DateTime dt;
            CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
            DateTimeStyles styles = DateTimeStyles.None;
            //DateTime.TryParse(strdt, culture, styles, out dt);
            DateTime.TryParseExact(strdt, "yyyyMMddHHmmss", culture, styles, out dt);
            jsonObj.DTG = dt ;
            
            jsonObj.data = new List<Test2SummaryData>();

            bool start = false;
            foreach (var item in lines)
            {
                if (start)
                {
                    var items = item.Split(",", StringSplitOptions.None);
                    if (items.Length == 10
                        )
                    {
                        try
                        {

                            jsonObj.data.Add(new Test2SummaryData
                            {
                                ClassName = items[0],
                                Name = items[1],
                                Max = double.Parse(items[2].Equals("") ? "0.0" : items[2]),
                                Value = items[3],
                                Min = double.Parse(items[4].Equals("") ? "0.0" : items[4]),
                                Time = double.Parse(items[5].Equals("") ? "0.0" : items[5]),
                                Status = items[6],
                                PN = items[7],
                                SN = items[8],
                                Messsage = items[9]
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception=" + e.StackTrace);

                            continue;
                        }
                    }
                }
                if (item.StartsWith("ClassName"))
                {
                    start = true;
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
}
