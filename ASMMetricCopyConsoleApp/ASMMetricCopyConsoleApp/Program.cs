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

namespace ASMMetricCopyConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("BigDataStorage"));
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudStorageAccount rawstorageAccount = CloudStorageAccount.Parse(Environment.GetEnvironmentVariable("KOTStorage"));
            CloudBlobClient rawblobClient = rawstorageAccount.CreateCloudBlobClient();
            CloudBlobContainer rawContainer = rawblobClient.GetContainerReference(Environment.GetEnvironmentVariable("RawKOTDataContainer"));
            CloudBlobContainer container = blobClient.GetContainerReference(Environment.GetEnvironmentVariable("KOTDataContainer"));

            Console.WriteLine("Hello World!");
        }
    }
}
