using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FileManager.Services
{
    public class DataLakeService
    {
        public DataLakeClient dataLakeClient;
        public DataLakeService() => dataLakeClient = new DataLakeClient();
    }
}
