using Azure;
using Azure.Storage;
using Azure.Storage.Files.DataLake;
using Azure.Storage.Files.DataLake.Models;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace FileManager.Services
{
    public class DataLakeClient
    {
        public void GetDataLakeServiceClient(ref DataLakeServiceClient dataLakeServiceClient, string accountName, string accountKey)
        {
            StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(accountName, accountKey);

            string dfsUri = $"https://{accountName}.dfs.core.windows.net";

            dataLakeServiceClient = new DataLakeServiceClient(new Uri(dfsUri), sharedKeyCredential);
        }


        public async Task<DataLakeFileSystemClient> GetFileSystem(DataLakeServiceClient serviceClient, string fileSystemName)
        {
            DataLakeFileSystemClient fsClient = serviceClient.GetFileSystemClient(fileSystemName);

            await fsClient.CreateIfNotExistsAsync();

            return fsClient;
        }


        public async Task<DataLakeDirectoryClient> GetDirectory(DataLakeServiceClient serviceClient, string fileSystemName, string directoryName)
        {
            DataLakeFileSystemClient fileSystemClient = await this.GetFileSystem(serviceClient, fileSystemName);

            DataLakeDirectoryClient directoryClient = fileSystemClient.GetDirectoryClient(directoryName);

            await directoryClient.CreateIfNotExistsAsync();

            return directoryClient;
        }


        public async Task UploadFile(DataLakeDirectoryClient directoryClient, string fileName, byte[] fileBytes)
        {
            DataLakeFileClient fileClient = directoryClient.GetFileClient(fileName);

            MemoryStream ms = new MemoryStream(fileBytes);
            await fileClient.UploadAsync(ms);
        }


        public async Task<string> DownloadFile(DataLakeDirectoryClient directoryClient, string fileName)
        {
            DataLakeFileClient fileClient = directoryClient.GetFileClient(fileName);

            Response<FileDownloadInfo> downloadResponse = await fileClient.ReadAsync();

            BinaryReader reader = new BinaryReader(downloadResponse.Value.Content);

            var tempPath = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString());

            using (FileStream fs = File.OpenWrite(tempPath))
            {
                int bufferSize = 4096;
                byte[] buffer = new byte[bufferSize];
                int count;

                while ((count = reader.Read(buffer, 0, buffer.Length)) != 0)
                {
                    fs.Write(buffer, 0, count);
                }

                await fs.FlushAsync();

                fs.Close();
            }

            return tempPath;
        }

        public void DeleteTempFile(string tempFilePath)
        {
            if (File.Exists(tempFilePath))
            {
                File.Delete(tempFilePath);
            }
        }


        public static async void SaveObjectToFile(string accountName, string accountKey, object objectToSave, string fileSystemName, params string[] folderPath)
        {
            DataLakeClient dataLakeClient = new DataLakeClient();
            DataLakeServiceClient serviceClient = null;
            dataLakeClient.GetDataLakeServiceClient(ref serviceClient, accountName, accountKey);


            DataLakeDirectoryClient dirClient = await dataLakeClient.GetDirectory(serviceClient, fileSystemName, folderPath[0]);
            foreach (var folder in folderPath.Skip(1))
            {
                dirClient = dirClient.GetSubDirectoryClient(folder);
            }

            string jsonString = JsonConvert.SerializeObject(objectToSave);
            byte[] fileBytes = System.Text.UTF8Encoding.Default.GetBytes(jsonString);

            await dataLakeClient.UploadFile(dirClient, Guid.NewGuid().ToString(), fileBytes);
        }
    }
}
