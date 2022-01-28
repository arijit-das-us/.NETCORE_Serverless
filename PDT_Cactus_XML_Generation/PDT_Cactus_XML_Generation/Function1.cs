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
using System.Xml.Linq;
using System.Text;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Producer;

namespace PDT_Cactus_XML_Generation
{
    public static class Function1
    {
        static EventHubProducerClient producerClient;

        [FunctionName("FHIR_Cactus_Affiliation_XMLGenerator")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
                log.LogInformation("C# HTTP trigger function processed a request.");

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("pdt_StorageAccount"));
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer rawContainer = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("pdt_StorageContainer"));

                 BlobContinuationToken blobContinuationToken = null;
                 do{
                    var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                    prefix: "pdt-cactus-eventhub",
                    useFlatBlobListing: true,
                    blobListingDetails: BlobListingDetails.All,
                    maxResults: null,
                    currentToken: blobContinuationToken,
                    options: null,
                    operationContext: null
                );
                log.LogInformation("resultSegment initiated");
                blobContinuationToken = resultSegment.ContinuationToken;
                var blobs = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv") && Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("Cactus_Affiliation")));
                log.LogInformation("Blob count = " + blobs.Count());
                foreach (var blob in blobs)
                {
                   log.LogInformation("inside for");
                    var block = blob as CloudBlockBlob;
                    
                    var blobPath = block.Name;
                    var fileName = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                    var filePath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                    log.LogInformation("filePath=" + filePath);
                    log.LogInformation("filename="+ fileName);
                    var content = await block.DownloadTextAsync();
                    //var sDestination =  filePath + '/' + fileName.Replace("csv","xml");

                    await sendFHIRXMLToEventHub(content, fileName, filePath);

                    /*
                    var xDocument = convertFHIRJsonToXML(content,fileName,filePath);
                    Console.WriteLine("uploading XML content to blob storage");

                    CloudBlockBlob cloudBlockBlob = rawContainer.GetBlockBlobReference(sDestination);
                    var memory = new MemoryStream();
                    xDocument.Save(memory);

                    await cloudBlockBlob.UploadTextAsync(Encoding.UTF8.GetString(memory.ToArray()));
                    //await block.DeleteAsync();
                    */
                    await block.DeleteAsync();
                }

                 }while (blobContinuationToken != null);

                return new OkResult();
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        public static async Task<IActionResult> sendFHIRXMLToEventHub(String content, string filename, string fileLocation)
        {

            string[] lines = content.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Console.WriteLine("line count=" + lines.Length);
            int BATCH_SIZE = Int32.Parse(Environment.GetEnvironmentVariable("pdt_BatchSize"));
            Console.WriteLine("batch size = " + BATCH_SIZE);
            int current_size = 0;
            var fhirAffxml = new XDocument(
                new XDeclaration("1.0", "UTF-8", "yes"));
            var xroot = new XElement("root"); //Create the root

            for (int i = 0; i < lines.Length; i++)
            {
                //Create each row
                //Console.WriteLine("i = "+i);

                XElement row = rowCreator(lines[i]);
                if (row != null)
                    xroot.Add(row);

                current_size++;
                if (current_size == BATCH_SIZE || i == lines.Length - 1)
                {
                    fhirAffxml.Add(xroot);
                    var memory = new MemoryStream();
                    fhirAffxml.Save(memory);
                    Console.WriteLine("created XML content for i=" + i);
                    //Console.WriteLine(Encoding.UTF8.GetString(memory.ToArray()));
                    await sendToEventHub(memory);

                    fhirAffxml = new XDocument(
                        new XDeclaration("1.0", "UTF-8", "yes"));
                    xroot = new XElement("root"); //Create the root
                    current_size = 0;
                }
            }

            return new OkResult();

        }

        public static async Task<IActionResult> sendToEventHub(MemoryStream memory)
        {

            string connectionString = Environment.GetEnvironmentVariable("pdt_EventHubConnectionString");
            string EH_cactus_affiliation = Environment.GetEnvironmentVariable("pdt_EH_cactus_affiliation");
            producerClient = new EventHubProducerClient(connectionString, EH_cactus_affiliation);

            // Create a batch of events 
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            //sending to Event Hub
            if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(memory.ToString()))))
            {
                // if it is too large for the batch
                throw new Exception($"Event is too large for the batch and cannot be sent.");
            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of events has been published.");
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
            return new OkResult();

        }
        private static XElement rowCreator(string row)
        {

            string[] fields = row.Split(',', StringSplitOptions.None);
            if (fields.Length == 24)
            {
                var xrow = new XElement("row");
                //Create the element var and Attributes with the field name and value

                var xvar = new XElement("provider",
                            new XElement("CACTUS_PROVIDER_K", fields[0].Replace("\"", "").Trim()),
                            new XElement("FACETS_PRCR_ID", fields[1].Replace("\"", "").Trim()),
                            new XElement("LAST_NAME", fields[2].Replace("\"", "").Trim()),
                            new XElement("FIRST_NAME", fields[3].Replace("\"", "").Trim()),
                            new XElement("MIDDLE_NAME", fields[4].Replace("\"", "").Trim()),
                            new XElement("MIDDLE_INITIAL", fields[5].Replace("\"", "").Trim()),
                            new XElement("SUFFIX", fields[6].Replace("\"", "").Trim()),
                            new XElement("NPI", fields[7].Replace("\"", "").Trim()),
                            new XElement("providerAffiliation",
                                new XElement("PROVIDER_K", fields[8].Replace("\"", "").Trim()),
                                new XElement("ENTITY_K", fields[9].Replace("\"", "").Trim()),
                                new XElement("DEPARTMENT", fields[10].Replace("\"", "").Trim()),
                                new XElement("AFFILIATION_TYPE", fields[11].Replace("\"", "").Trim()),
                                new XElement("INSTITUTION_K", fields[12].Replace("\"", "").Trim()),
                                new XElement("CONTACT_NAME", fields[13].Replace("\"", "").Trim()),
                                new XElement("CONTACT_PHONE", fields[14].Replace("\"", "").Trim()),
                                new XElement("CONTACT_FAX", fields[15].Replace("\"", "").Trim()),
                                new XElement("CONTACT_EMAIL", fields[16].Replace("\"", "").Trim()),
                                new XElement("INSTITUTION_TYPE", fields[17].Replace("\"", "").Trim()),
                                new XElement("CATEGORY", fields[18].Replace("\"", "").Trim()),
                                new XElement("CLOSED", fields[19].Replace("\"", "").Trim()),
                                new XElement("START_DATE", fields[20].Replace("\"", "").Trim()),
                                new XElement("FINISH_DATE", fields[21].Replace("\"", "").Trim()),
                                new XElement("ACTIVE", fields[22].Replace("\"", "").Trim()),
                                new XElement("PERSISTENT_VERIFICATIONS", fields[23].Replace("\"", "").Trim())
                        )
                    );

                xrow.Add(xvar);

                return xrow;
            }
            else
            {
                return null;
            }
        }
    }

    public static class Function2
    {


        static EventHubProducerClient producerClient;

        [FunctionName("FHIR_Cactus_Qualification_XMLGenerator")]


        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
                log.LogInformation("C# HTTP trigger function processed a request.");

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("pdt_StorageAccount"));
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer rawContainer = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("pdt_StorageContainer"));

                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                        prefix: "pdt-cactus-eventhub",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: blobContinuationToken,
                        options: null,
                        operationContext: null
                    );

                    blobContinuationToken = resultSegment.ContinuationToken;
                    var blobs = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv") && Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("Cactus_Qualification")));
                    log.LogInformation("Blob count = " + blobs.Count());
                    foreach (var blob in blobs)
                    {
                        var block = blob as CloudBlockBlob;
                        var content = await block.DownloadTextAsync();
                        var blobPath = block.Name;
                        var fileName = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                        var filePath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                        //var sDestination =  filePath + '/' + fileName.Replace("csv","xml");

                        await sendFHIRXMLToEventHub(content, fileName, filePath);

                        /*
                        var xDocument = convertFHIRJsonToXML(content,fileName,filePath);
                        Console.WriteLine("uploading XML content to blob storage");

                        CloudBlockBlob cloudBlockBlob = rawContainer.GetBlockBlobReference(sDestination);
                        var memory = new MemoryStream();
                        xDocument.Save(memory);

                        await cloudBlockBlob.UploadTextAsync(Encoding.UTF8.GetString(memory.ToArray()));
                        //await block.DeleteAsync();
                        */
                        await block.DeleteAsync();
                    }

                } while (blobContinuationToken != null);

                return new OkResult();
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        public static async Task<IActionResult> sendFHIRXMLToEventHub(String content, string filename, string fileLocation)
        {

            string[] lines = content.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Console.WriteLine("line count=" + lines.Length);
            int BATCH_SIZE = Int32.Parse(Environment.GetEnvironmentVariable("pdt_BatchSize"));
            Console.WriteLine("batch size = " + BATCH_SIZE);
            int current_size = 0;
            var fhirAffxml = new XDocument(
                new XDeclaration("1.0", "UTF-8", "yes"));
            var xroot = new XElement("root"); //Create the root

            for (int i = 0; i < lines.Length; i++)
            {
                //Create each row
                //Console.WriteLine("i = "+i);

                XElement row = rowCreator(lines[i]);
                if (row != null)
                    xroot.Add(row);

                current_size++;
                if (current_size == BATCH_SIZE || i == lines.Length - 1)
                {
                    fhirAffxml.Add(xroot);
                    var memory = new MemoryStream();
                    fhirAffxml.Save(memory);
                    Console.WriteLine("created XML content for i=" + i);
                    //Console.WriteLine(Encoding.UTF8.GetString(memory.ToArray()));
                    await sendToEventHub(memory);

                    fhirAffxml = new XDocument(
                        new XDeclaration("1.0", "UTF-8", "yes"));
                    xroot = new XElement("root"); //Create the root
                    current_size = 0;
                }
            }

            return new OkResult();

        }

        public static async Task<IActionResult> sendToEventHub(MemoryStream memory)
        {

            string connectionString = Environment.GetEnvironmentVariable("pdt_EventHubConnectionString");
            string EH_cactus_qualification = Environment.GetEnvironmentVariable("pdt_EH_cactus_qualification");
            producerClient = new EventHubProducerClient(connectionString, EH_cactus_qualification);

            // Create a batch of events 
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            //sending to Event Hub
            if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(memory.ToString()))))
            {
                // if it is too large for the batch
                throw new Exception($"Event is too large for the batch and cannot be sent.");
            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of events has been published.");
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
            return new OkResult();

        }

        /*
                private static XDocument convertFHIRJsonToXML(String content, string filename, string fileLocation){

                    string[] lines = content.Split(Environment.NewLine,StringSplitOptions.RemoveEmptyEntries);
                    Console.WriteLine("line count="+lines.Length);

                    var fhirAffxml = new XDocument(
                        new XDeclaration("1.0", "UTF-8", "yes"));
                    var xroot = new XElement("root"); //Create the root
                    for (int i = 0; i < lines.Length; i++)
                    {
                        //Create each row
                        //Console.WriteLine("i = "+i);
                        xroot.Add(rowCreator(lines[i]));

                    }
                    fhirAffxml.Add(xroot);

                    Console.WriteLine("created XML content");

                    return fhirAffxml;
                }
                */

        private static XElement rowCreator(string row)
        {

            string[] fields = row.Split(',', StringSplitOptions.None);
            if (fields.Length == 35)
            {
                var xrow = new XElement("row");
                //Create the element var and Attributes with the field name and value
                if (fields[30].Replace("\"", "").Trim().StartsWith("Residency"))
                {
                    var xvar = new XElement("provider",
                                new XElement("CACTUS_PROVIDER_K", fields[0].Replace("\"", "").Trim()),
                                new XElement("FACETS_PRCR_ID", fields[1].Replace("\"", "").Trim()),
                                new XElement("LAST_NAME", fields[2].Replace("\"", "").Trim()),
                                new XElement("FIRST_NAME", fields[3].Replace("\"", "").Trim()),
                                new XElement("MIDDLE_NAME", fields[4].Replace("\"", "").Trim()),
                                new XElement("MIDDLE_INITIAL", fields[5].Replace("\"", "").Trim()),
                                new XElement("SUFFIX", fields[6].Replace("\"", "").Trim()),
                                new XElement("NPI", fields[7].Replace("\"", "").Trim()),
                                new XElement("boardCertification",
                                    new XElement("CERTIFICATION_DATE", fields[8].Replace("\"", "").Trim()),
                                    new XElement("EXPIRATION_DATE", fields[9].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED", fields[13].Replace("\"", "").Trim()),
                                    new XElement("DESCRIPTION", fields[10].Replace("\"", "").Trim()),
                                    new XElement("ACTIVE", fields[11].Replace("\"", "").Trim()),
                                    new XElement("SPECIALTY", fields[12].Replace("\"", "").Trim()),
                                    new XElement("ISSUER_ID", fields[14].Replace("\"", "").Trim()),
                                    new XElement("BOARD_CERTIFICATION_INDICATOR", fields[15].Replace("\"", "").Trim())),
                                new XElement("stateLicense",
                                    new XElement("LICENSE_NUMBER", fields[16].Replace("\"", "").Trim()),
                                    new XElement("LICENSE_STATE", fields[17].Replace("\"", "").Trim()),
                                    new XElement("LICENSE_CERTIFICATE", fields[18].Replace("\"", "").Trim()),
                                    new XElement("EXPIRATION_DATE", fields[19].Replace("\"", "").Trim()),
                                    new XElement("AWARDED_DATE", fields[20].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED", fields[21].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED_DATE", fields[22].Replace("\"", "").Trim()),
                                    new XElement("STATUS", fields[23].Replace("\"", "").Trim()),
                                    new XElement("ACTIVE", fields[24].Replace("\"", "").Trim())),
                                new XElement("residency",
                                    new XElement("DEGREE", fields[25].Replace("\"", "").Trim()),
                                    new XElement("DEGREE_TYPE", fields[26].Replace("\"", "").Trim()),
                                    new XElement("EDUCATION_TYPE", fields[27].Replace("\"", "").Trim()),
                                    new XElement("SPECIALTY", fields[28].Replace("\"", "").Trim()),
                                    new XElement("TITLE", fields[29].Replace("\"", "").Trim()),
                                    new XElement("PROGRAM", fields[30].Replace("\"", "").Trim()),
                                    new XElement("START_DATE", fields[31].Replace("\"", "").Trim()),
                                    new XElement("END_DATE", fields[32].Replace("\"", "").Trim()),
                                    new XElement("GRADUATION_COMPLETED", fields[33].Replace("\"", "").Trim()),
                                    new XElement("ISSUER_ID", fields[34].Replace("\"", "").Trim())),
                                new XElement("degree",
                                    new XElement("DEGREE", fields[25].Replace("\"", "").Trim())
                            )
                        );

                    xrow.Add(xvar);
                }
                else if (fields[30].Replace("\"", "").Trim().StartsWith("Internship"))
                {
                    var xvar = new XElement("provider",
                                new XElement("CACTUS_PROVIDER_K", fields[0].Replace("\"", "").Trim()),
                                new XElement("FACETS_PRCR_ID", fields[1].Replace("\"", "").Trim()),
                                new XElement("LAST_NAME", fields[2].Replace("\"", "").Trim()),
                                new XElement("FIRST_NAME", fields[3].Replace("\"", "").Trim()),
                                new XElement("MIDDLE_NAME", fields[4].Replace("\"", "").Trim()),
                                new XElement("MIDDLE_INITIAL", fields[5].Replace("\"", "").Trim()),
                                new XElement("SUFFIX", fields[6].Replace("\"", "").Trim()),
                                new XElement("NPI", fields[7].Replace("\"", "").Trim()),
                                new XElement("boardCertification",
                                    new XElement("CERTIFICATION_DATE", fields[8].Replace("\"", "").Trim()),
                                    new XElement("EXPIRATION_DATE", fields[9].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED", fields[13].Replace("\"", "").Trim()),
                                    new XElement("DESCRIPTION", fields[10].Replace("\"", "").Trim()),
                                    new XElement("ACTIVE", fields[11].Replace("\"", "").Trim()),
                                    new XElement("SPECIALTY", fields[12].Replace("\"", "").Trim()),
                                    new XElement("ISSUER_ID", fields[14].Replace("\"", "").Trim()),
                                    new XElement("BOARD_CERTIFICATION_INDICATOR", fields[15].Replace("\"", "").Trim())),
                                new XElement("stateLicense",
                                    new XElement("LICENSE_NUMBER", fields[16].Replace("\"", "").Trim()),
                                    new XElement("LICENSE_STATE", fields[17].Replace("\"", "").Trim()),
                                    new XElement("LICENSE_CERTIFICATE", fields[18].Replace("\"", "").Trim()),
                                    new XElement("EXPIRATION_DATE", fields[19].Replace("\"", "").Trim()),
                                    new XElement("AWARDED_DATE", fields[20].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED", fields[21].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED_DATE", fields[22].Replace("\"", "").Trim()),
                                    new XElement("STATUS", fields[23].Replace("\"", "").Trim()),
                                    new XElement("ACTIVE", fields[24].Replace("\"", "").Trim())),
                                new XElement("internship",
                                    new XElement("DEGREE", fields[25].Replace("\"", "").Trim()),
                                    new XElement("DEGREE_TYPE", fields[26].Replace("\"", "").Trim()),
                                    new XElement("EDUCATION_TYPE", fields[27].Replace("\"", "").Trim()),
                                    new XElement("SPECIALTY", fields[28].Replace("\"", "").Trim()),
                                    new XElement("TITLE", fields[29].Replace("\"", "").Trim()),
                                    new XElement("PROGRAM", fields[30].Replace("\"", "").Trim()),
                                    new XElement("START_DATE", fields[31].Replace("\"", "").Trim()),
                                    new XElement("END_DATE", fields[32].Replace("\"", "").Trim()),
                                    new XElement("GRADUATION_COMPLETED", fields[33].Replace("\"", "").Trim()),
                                    new XElement("ISSUER_ID", fields[34].Replace("\"", "").Trim())),
                                new XElement("degree",
                                    new XElement("DEGREE", fields[25].Replace("\"", "").Trim())
                            )
                        );

                    xrow.Add(xvar);
                }
                else if (fields[30].Replace("\"", "").Trim().StartsWith("Medical"))
                {
                    var xvar = new XElement("provider",
                                new XElement("CACTUS_PROVIDER_K", fields[0].Replace("\"", "").Trim()),
                                new XElement("FACETS_PRCR_ID", fields[1].Replace("\"", "").Trim()),
                                new XElement("LAST_NAME", fields[2].Replace("\"", "").Trim()),
                                new XElement("FIRST_NAME", fields[3].Replace("\"", "").Trim()),
                                new XElement("MIDDLE_NAME", fields[4].Replace("\"", "").Trim()),
                                new XElement("MIDDLE_INITIAL", fields[5].Replace("\"", "").Trim()),
                                new XElement("SUFFIX", fields[6].Replace("\"", "").Trim()),
                                new XElement("NPI", fields[7].Replace("\"", "").Trim()),
                                new XElement("boardCertification",
                                    new XElement("CERTIFICATION_DATE", fields[8].Replace("\"", "").Trim()),
                                    new XElement("EXPIRATION_DATE", fields[9].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED", fields[13].Replace("\"", "").Trim()),
                                    new XElement("DESCRIPTION", fields[10].Replace("\"", "").Trim()),
                                    new XElement("ACTIVE", fields[11].Replace("\"", "").Trim()),
                                    new XElement("SPECIALTY", fields[12].Replace("\"", "").Trim()),
                                    new XElement("ISSUER_ID", fields[14].Replace("\"", "").Trim()),
                                    new XElement("BOARD_CERTIFICATION_INDICATOR", fields[15].Replace("\"", "").Trim())),
                                new XElement("stateLicense",
                                    new XElement("LICENSE_NUMBER", fields[16].Replace("\"", "").Trim()),
                                    new XElement("LICENSE_STATE", fields[17].Replace("\"", "").Trim()),
                                    new XElement("LICENSE_CERTIFICATE", fields[18].Replace("\"", "").Trim()),
                                    new XElement("EXPIRATION_DATE", fields[19].Replace("\"", "").Trim()),
                                    new XElement("AWARDED_DATE", fields[20].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED", fields[21].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED_DATE", fields[22].Replace("\"", "").Trim()),
                                    new XElement("STATUS", fields[23].Replace("\"", "").Trim()),
                                    new XElement("ACTIVE", fields[24].Replace("\"", "").Trim())),
                                new XElement("medicalSchool",
                                    new XElement("DEGREE", fields[25].Replace("\"", "").Trim()),
                                    new XElement("DEGREE_TYPE", fields[26].Replace("\"", "").Trim()),
                                    new XElement("EDUCATION_TYPE", fields[27].Replace("\"", "").Trim()),
                                    new XElement("SPECIALTY", fields[28].Replace("\"", "").Trim()),
                                    new XElement("TITLE", fields[29].Replace("\"", "").Trim()),
                                    new XElement("PROGRAM", fields[30].Replace("\"", "").Trim()),
                                    new XElement("START_DATE", fields[31].Replace("\"", "").Trim()),
                                    new XElement("END_DATE", fields[32].Replace("\"", "").Trim()),
                                    new XElement("GRADUATION_COMPLETED", fields[33].Replace("\"", "").Trim()),
                                    new XElement("ISSUER_ID", fields[34].Replace("\"", "").Trim())),
                                new XElement("degree",
                                    new XElement("DEGREE", fields[25].Replace("\"", "").Trim())
                            )
                        );

                    xrow.Add(xvar);
                }
                else
                {
                    var xvar = new XElement("provider",
                                new XElement("CACTUS_PROVIDER_K", fields[0].Replace("\"", "").Trim()),
                                new XElement("FACETS_PRCR_ID", fields[1].Replace("\"", "").Trim()),
                                new XElement("LAST_NAME", fields[2].Replace("\"", "").Trim()),
                                new XElement("FIRST_NAME", fields[3].Replace("\"", "").Trim()),
                                new XElement("MIDDLE_NAME", fields[4].Replace("\"", "").Trim()),
                                new XElement("MIDDLE_INITIAL", fields[5].Replace("\"", "").Trim()),
                                new XElement("SUFFIX", fields[6].Replace("\"", "").Trim()),
                                new XElement("NPI", fields[7].Replace("\"", "").Trim()),
                                new XElement("boardCertification",
                                    new XElement("CERTIFICATION_DATE", fields[8].Replace("\"", "").Trim()),
                                    new XElement("EXPIRATION_DATE", fields[9].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED", fields[13].Replace("\"", "").Trim()),
                                    new XElement("DESCRIPTION", fields[10].Replace("\"", "").Trim()),
                                    new XElement("ACTIVE", fields[11].Replace("\"", "").Trim()),
                                    new XElement("SPECIALTY", fields[12].Replace("\"", "").Trim()),
                                    new XElement("ISSUER_ID", fields[14].Replace("\"", "").Trim()),
                                    new XElement("BOARD_CERTIFICATION_INDICATOR", fields[15].Replace("\"", "").Trim())),
                                new XElement("stateLicense",
                                    new XElement("LICENSE_NUMBER", fields[16].Replace("\"", "").Trim()),
                                    new XElement("LICENSE_STATE", fields[17].Replace("\"", "").Trim()),
                                    new XElement("LICENSE_CERTIFICATE", fields[18].Replace("\"", "").Trim()),
                                    new XElement("EXPIRATION_DATE", fields[19].Replace("\"", "").Trim()),
                                    new XElement("AWARDED_DATE", fields[20].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED", fields[21].Replace("\"", "").Trim()),
                                    new XElement("VERIFIED_DATE", fields[22].Replace("\"", "").Trim()),
                                    new XElement("STATUS", fields[23].Replace("\"", "").Trim()),
                                    new XElement("ACTIVE", fields[24].Replace("\"", "").Trim())),
                                new XElement("degree",
                                    new XElement("DEGREE", fields[25].Replace("\"", "").Trim())
                            )
                        );

                    xrow.Add(xvar);
                }

                return xrow;
            }
            else
            {
                return null;
            }
        }

    }


    public static class Function3
    {


        static EventHubProducerClient producerClient;

        [FunctionName("FHIR_Cactus_Institution_XMLGenerator")]


        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            try
            {
                log.LogInformation("C# HTTP trigger function processed a request.");

                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("pdt_StorageAccount"));
                CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                CloudBlobContainer rawContainer = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("pdt_StorageContainer"));

                BlobContinuationToken blobContinuationToken = null;
                do
                {
                    var resultSegment = await rawContainer.ListBlobsSegmentedAsync(
                        prefix: "pdt-cactus-eventhub",
                        useFlatBlobListing: true,
                        blobListingDetails: BlobListingDetails.All,
                        maxResults: null,
                        currentToken: blobContinuationToken,
                        options: null,
                        operationContext: null
                    );

                    blobContinuationToken = resultSegment.ContinuationToken;
                    var blobs = resultSegment.Results.Where(b => (Path.GetExtension(b.Uri.AbsolutePath).Equals(".csv") && Path.GetFileNameWithoutExtension(b.Uri.AbsolutePath).StartsWith("Cactus_Institutions")));
                    log.LogInformation("Blob count = " + blobs.Count());
                    foreach (var blob in blobs)
                    {
                        var block = blob as CloudBlockBlob;
                        var content = await block.DownloadTextAsync();
                        var blobPath = block.Name;
                        var fileName = blobPath.Substring(blobPath.LastIndexOf('/') + 1);
                        var filePath = blobPath.Substring(0, blobPath.LastIndexOf('/'));
                        //var sDestination =  filePath + '/' + fileName.Replace("csv","xml");

                        await sendFHIRXMLToEventHub(content, fileName, filePath);

                        /*
                        var xDocument = convertFHIRJsonToXML(content,fileName,filePath);
                        Console.WriteLine("uploading XML content to blob storage");

                        CloudBlockBlob cloudBlockBlob = rawContainer.GetBlockBlobReference(sDestination);
                        var memory = new MemoryStream();
                        xDocument.Save(memory);

                        await cloudBlockBlob.UploadTextAsync(Encoding.UTF8.GetString(memory.ToArray()));
                        //await block.DeleteAsync();
                        */
                        await block.DeleteAsync();
                    }

                } while (blobContinuationToken != null);

                return new OkResult();
            }
            catch (Exception ex)
            {
                log.LogInformation(ex.StackTrace);
                return new BadRequestObjectResult(ex);
            }
        }

        public static async Task<IActionResult> sendFHIRXMLToEventHub(String content, string filename, string fileLocation)
        {

            string[] lines = content.Split(Environment.NewLine, StringSplitOptions.RemoveEmptyEntries);
            Console.WriteLine("line count=" + lines.Length);
            int BATCH_SIZE = Int32.Parse(Environment.GetEnvironmentVariable("pdt_BatchSize"));
            Console.WriteLine("batch size = " + BATCH_SIZE);
            int current_size = 0;
            var fhirAffxml = new XDocument(
                new XDeclaration("1.0", "UTF-8", "yes"));
            var xroot = new XElement("root"); //Create the root

            for (int i = 0; i < lines.Length; i++)
            {
                //Create each row
                //Console.WriteLine("i = "+i);
                XElement row = rowCreator(lines[i]);
                if(row != null)
                    xroot.Add(row);

                current_size++;
                if (current_size == BATCH_SIZE || i == lines.Length - 1)
                {
                    fhirAffxml.Add(xroot);
                    var memory = new MemoryStream();
                    fhirAffxml.Save(memory);
                    Console.WriteLine("created XML content for i=" + i);
                    //Console.WriteLine(Encoding.UTF8.GetString(memory.ToArray()));
                    await sendToEventHub(memory);

                    fhirAffxml = new XDocument(
                        new XDeclaration("1.0", "UTF-8", "yes"));
                    xroot = new XElement("root"); //Create the root
                    current_size = 0;
                }
            }

            return new OkResult();

        }

        public static async Task<IActionResult> sendToEventHub(MemoryStream memory)
        {

            string connectionString = Environment.GetEnvironmentVariable("pdt_EventHubConnectionString");
            string EH_cactus_institution = Environment.GetEnvironmentVariable("pdt_EH_cactus_institution");
            producerClient = new EventHubProducerClient(connectionString, EH_cactus_institution);

            // Create a batch of events 
            using EventDataBatch eventBatch = await producerClient.CreateBatchAsync();

            //sending to Event Hub
            if (!eventBatch.TryAdd(new EventData(Encoding.UTF8.GetBytes(memory.ToString()))))
            {
                // if it is too large for the batch
                throw new Exception($"Event is too large for the batch and cannot be sent.");
            }

            try
            {
                // Use the producer client to send the batch of events to the event hub
                await producerClient.SendAsync(eventBatch);
                Console.WriteLine($"A batch of events has been published.");
            }
            finally
            {
                await producerClient.DisposeAsync();
            }
            return new OkResult();

        }

        /*
                private static XDocument convertFHIRJsonToXML(String content, string filename, string fileLocation){

                    string[] lines = content.Split(Environment.NewLine,StringSplitOptions.RemoveEmptyEntries);
                    Console.WriteLine("line count="+lines.Length);

                    var fhirAffxml = new XDocument(
                        new XDeclaration("1.0", "UTF-8", "yes"));
                    var xroot = new XElement("root"); //Create the root
                    for (int i = 0; i < lines.Length; i++)
                    {
                        //Create each row
                        //Console.WriteLine("i = "+i);
                        xroot.Add(rowCreator(lines[i]));

                    }
                    fhirAffxml.Add(xroot);

                    Console.WriteLine("created XML content");

                    return fhirAffxml;
                }
                */

        private static XElement rowCreator(string row)
        {

            string[] fields = row.Split(',', StringSplitOptions.None);

            if (fields.Length == 18)
            {
                var xrow = new XElement("row");
                //Create the element var and Attributes with the field name and value

                var xvar = new XElement("institution",
                            new XElement("INSTITUTION_K", fields[0].Replace("\"", "").Trim()),
                            new XElement("LOOKUP", fields[1].Replace("\"", "").Trim()),
                            new XElement("AMAINSTITUTIONCODE", fields[2].Replace("\"", "").Trim()),
                            new XElement("NAME", fields[3].Replace("\"", "").Trim()),
                            new XElement("SALUTATION", fields[4].Replace("\"", "").Trim()),
                            new XElement("CONTACT", fields[5].Replace("\"", "").Trim()),
                            new XElement("ADDRESS1", fields[6].Replace("\"", "").Trim()),
                            new XElement("ADDRESS2", fields[7].Replace("\"", "").Trim()),
                            new XElement("CITY", fields[8].Replace("\"", "").Trim()),
                            new XElement("STATE", fields[9].Replace("\"", "").Trim()),
                            new XElement("ZIP_CODE", fields[10].Replace("\"", "").Trim()),
                            new XElement("COUNTRY", fields[11].Replace("\"", "").Trim()),
                            new XElement("PHONE_PRIMARY", fields[12].Replace("\"", "").Trim()),
                            new XElement("PHONE_ALTERNATIVE", fields[13].Replace("\"", "").Trim()),
                            new XElement("FAX", fields[14].Replace("\"", "").Trim()),
                            new XElement("EMAIL", fields[15].Replace("\"", "").Trim()),
                            new XElement("ACTIVE", fields[16].Replace("\"", "").Trim()),
                            new XElement("NPI", fields[17].Replace("\"", "").Trim())
                    );

                xrow.Add(xvar);

                return xrow;
            }
            else
            {
                return null;
            }
        }

    }

}
