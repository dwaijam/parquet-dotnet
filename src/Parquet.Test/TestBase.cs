using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;

namespace Parquet.Test
{
   public class TestBase
   {
      protected string GetDataFilePath(string name)
      {
         var codeBaseUrl = new Uri(Assembly.GetExecutingAssembly().CodeBase);
         string codeBasePath = Uri.UnescapeDataString(codeBaseUrl.AbsolutePath);
         string dirPath = Path.GetDirectoryName(codeBasePath);
         return Path.Combine(dirPath, "data", name);
      }
   }
}