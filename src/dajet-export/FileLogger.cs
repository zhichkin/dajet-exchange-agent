using System;
using System.IO;
using System.Reflection;
using System.Text;

namespace DaJet.Export
{
    internal static class FileLogger
    {
        private static string _filePath;
        private static object _syncLog = new object();
        private static string GetFilePath()
        {
            if (_filePath != null)
            {
                return _filePath;
            }

            Assembly asm = Assembly.GetExecutingAssembly();
            string appCatalogPath = Path.GetDirectoryName(asm.Location);
            _filePath = Path.Combine(appCatalogPath, "dajet-export.log");

            return _filePath;
        }
        internal static int LogSize { get; set; } = 102400;
        internal static void Log(string text)
        {
            lock (_syncLog)
            {
                LogSyncronized(text);
            }
        }
        internal static void LogSyncronized(string text)
        {
            string filePath = GetFilePath();
            FileInfo file = new FileInfo(filePath);

            try
            {
                if (file.Exists && file.Length > LogSize)
                {
                    file.Delete();
                }
            }
            catch (Exception ex)
            {
                text += Environment.NewLine + GetErrorText(ex);
            }

            using (StreamWriter writer = new StreamWriter(GetFilePath(), true, Encoding.UTF8))
            {
                writer.WriteLine("[{0}] {1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), text);
            }
        }
        private static string GetErrorText(Exception ex)
        {
            string errorText = string.Empty;
            Exception error = ex;
            while (error != null)
            {
                errorText += (errorText == string.Empty) ? error.Message : Environment.NewLine + error.Message;
                error = error.InnerException;
            }
            return errorText;
        }
    }
}