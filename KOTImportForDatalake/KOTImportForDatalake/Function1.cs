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

namespace KOTImportForDatalake
{
    public static class Function1
    {
        [FunctionName("KOTImportASMOptLog")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("BigDataStorage"));
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("KOTStorage"));
            CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawKOTDataContainer"));
            CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("KOTDataContainer"));

            string datetime = req.Query["yearMonthDay"];

            string sheaderNOM = req.Headers["yearMonthDay"];
            

            datetime = datetime ?? sheaderNOM ?? DateTime.Now.Year.ToString() + "-" + DateTime.Now.Month.ToString() + "-" + DateTime.Now.Day.ToString();


            log.LogInformation("date time = " + datetime);
            var dateArray = datetime.Split("-", StringSplitOptions.None);
            var sContainerPath = dateArray.Length == 3 ? "year=" + dateArray[0] + "/month=" + dateArray[1] + "/day=" + dateArray[2] : (dateArray.Length == 2 ? "year=" + dateArray[0] + "/month=" + dateArray[1] : (dateArray.Length == 1 ? "year=" + dateArray[0]:"/"));
            log.LogInformation("path = " + sContainerPath);

            BlobContinuationToken blobContinuationToken = null;
            do
            {
                var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                    prefix: "",
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.All,
                    maxResults: null,
                    currentToken: blobContinuationToken,
                    options: null,
                    operationContext: null
                );
                    // Get the value of the continuation token returned by the listing call.
                    /*
                    blobContinuationToken = resultSegment.ContinuationToken;
                    var blobs = resultSegment.Results.Where(b => (Path.GetDirectoryName(b.Uri.AbsolutePath).Contains(sContainerPath) && Path.GetExtension(b.Uri.AbsolutePath).Equals(".json") && Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("ASMOptimizationLog")));
                    log.LogInformation("blob count total for ASM Self Opt files=" + blobs.Count());

                    foreach (var blob in blobs)
                    {
                        var block = blob as CloudBlockBlob;

                        var content = await block.DownloadTextAsync();

                        var blobPath = block.Name;
                        var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                        var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                        var sDestination = "ASMOptLogData/" + block.Name;
                        CloudBlockBlob cloudBlockBlobRaw = container.GetBlockBlobReference(sDestination);
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
                            Console.WriteLine($"Blob: {cloudBlockBlobRaw.Name} Complete");

                        }
                        catch (Exception ex)
                        {
                            log.LogInformation(ex.Message);
                        }
                    }
                    */
                    // Get the value of the continuation token returned by the listing call.
                    blobContinuationToken = resultSegment.ContinuationToken;
                    var blobsASMMetric = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".json") && Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("ASMMetrics")));
                    log.LogInformation("blob count total for ASM metric data files=" + blobsASMMetric.Count());

                    foreach (var blob in blobsASMMetric)
                    {
                        var block = blob as CloudBlockBlob;

                        var content = await block.DownloadTextAsync();

                        var blobPath = block.Name;
                        var filename = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                        var filepath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                        var sDestination = "ASMMetricData/" + block.Name;
                        CloudBlockBlob cloudBlockBlobRaw = container.GetBlockBlobReference(sDestination);
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
                            Console.WriteLine($"Blob: {cloudBlockBlobRaw.Name} Complete");

                        }
                        catch (Exception ex)
                        {
                            log.LogInformation(ex.Message);
                        }
                    }
                } while (blobContinuationToken != null); // Loop while the continuation token is not null.              

            return new OkObjectResult("");
        }
        catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
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

