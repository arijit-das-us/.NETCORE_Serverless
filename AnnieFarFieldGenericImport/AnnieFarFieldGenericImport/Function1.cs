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

/*using System.Timers;
using System.Threading;
using System;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;
*/


namespace AnnieFarFieldGenericImport
{
    public static class Function1
    {
        [FunctionName("CopyAnnieFarFieldData")]
        public static async Task RunAsync([TimerTrigger("0 5 */1 * * *")]TimerInfo myTimer, ILogger log)
        {
            try
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("BigDataStorage"));
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("W2sakcsStorage"));
                CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
                CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawAnnieDataContainer"));
                CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("AnnieDataContainer"));


                string datetime = DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString();


                log.LogInformation("date time = " + datetime);

                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                        prefix: "annie/",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: blobContinuationToken,
                        options: null,
                        operationContext: null
                    );

                    // Get the value of the continuation token returned by the listing call.
                    blobContinuationToken = resultSegment.ContinuationToken;
                    var blobs = resultSegment.Results.Where(b => (!Path.GetExtension(b.Uri.AbsolutePath).Contains("Archive") && Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv")));
                    log.LogInformation("blob count total for Annie files=" + blobs.Count());

                    foreach (var blob in blobs)
                    {
                        var block = blob as CloudBlockBlob;

                        var content = await block.DownloadTextAsync();

                        var blobPath = block.Name;
                        var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                        var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                        var sDestination = "AnnieFarFieldData/" + block.Name.Replace(".csv", ".json");
                        log.LogInformation("Original abs file path=" + blobPath);
                        log.LogInformation("destination file path=" + sDestination);

                        var metadataAbsfilename = block.Name.Replace(".csv", "_metadata.json");
                        var metadatablob = rawContainer.GetBlockBlobReference(metadataAbsfilename);

                        var metadata = await metadatablob.DownloadTextAsync();
                        log.LogInformation("downloading metadata");
                        var jsonObj = GetAnnieFarFieldSummaryData(content, filename, filepath, metadata);
                        log.LogInformation("popultaed Annie data in json");
                        CloudBlockBlob cloudBlockBlob = container.GetBlockBlobReference(sDestination);
                        try
                        {
                            System.Text.StringBuilder sb = new System.Text.StringBuilder();
                            StringWriter sw = new StringWriter(sb);

                            using (JsonWriter writer = new JsonTextWriter(sw))
                            {
                                var serializer = new JsonSerializer();
                                serializer.Serialize(writer, jsonObj);
                            }

                            //var js = JsonConvert.SerializeObject(jsonObj);
                            log.LogInformation("serialize json");
                            var task = Task.Run(async () => { await cloudBlockBlob.UploadTextAsync(sb.ToString()); });
                            log.LogInformation("blob uploaded");
                        }
                        catch (Exception ex) { log.LogInformation(ex.Message); }
                    }
                } while (blobContinuationToken != null); // Loop while the continuation token is not null.
               // return new OkResult();

            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
               // return new BadRequestObjectResult(ex);
            }
        }

        private static AnnieFarFieldSummary GetAnnieFarFieldSummaryData(String content, string filename, string filelocation, string metadata)
        {
            string[] lines = content.Split(Environment.NewLine);


            var jsonObj = JsonConvert.DeserializeObject<AnnieFarFieldSummary>(metadata);

            jsonObj.OriginalFilename = filename.Trim();

            jsonObj.FileLocation = filelocation.Trim();

            jsonObj.ASMSN = lines.First(l => l.StartsWith("! Serial Number")).Split(":")[1].Trim();
            string strdt = lines.First(l => l.StartsWith("! Date")).Substring(lines.First(l => l.StartsWith("! Date")).IndexOf(':') + 1).Trim();
            DateTime dt;
            CultureInfo culture = CultureInfo.CreateSpecificCulture("en-US");
            DateTimeStyles styles = DateTimeStyles.None;
            DateTime.TryParse(strdt, culture, styles, out dt);
            //DateTime.TryParseExact(strdt, "yyyy/MM/dd hh:mm:ss tt", culture, styles, out dt);
            jsonObj.TestDateTime = dt;
            string freq = lines.First(l => l.StartsWith("! Frequency")).Substring(12).Trim();
            jsonObj.Frequency = freq;
            jsonObj.data = new List<AnnieFarFieldSummaryData>();

            bool start = false;
            foreach (var item in lines)
            {
                if (start)
                {
                    var items = item.Split(",", StringSplitOptions.None);
                    if (items.Length == 6
                        )
                    {
                        try
                        {

                            jsonObj.data.Add(new AnnieFarFieldSummaryData
                            {
                                Tertiary = double.Parse(items[0].Equals("") ? "0.0" : items[0]),
                                Secondary = double.Parse(items[1].Equals("") ? "0.0" : items[1]),
                                Primary = double.Parse(items[2].Equals("") ? "0.0" : items[2]),
                                Frequency = double.Parse(items[3].Equals("") ? "0.0" : items[3]),
                                Mag_dB = double.Parse(items[4].Equals("") ? "0.0" : items[4]),
                                Phase_deg = double.Parse(items[5].Equals("") ? "0.0" : items[5])
                            });
                        }
                        catch (Exception e)
                        {
                            Console.WriteLine("exception=" + e.StackTrace);

                            continue;
                        }
                    }
                }
                if (item.StartsWith("Tertiary"))
                {
                    start = true;
                }
            }

            return jsonObj;
        }

    }
}
