using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Text;

namespace DaJet.FileLogger
{
    public static class FileLogger
    {
        private static readonly string _filePath;
        private static readonly string _catalogPath;
        private static readonly object _syncLog = new object();

        static FileLogger()
        {
            Assembly asm = Assembly.GetExecutingAssembly();
            _catalogPath = Path.GetDirectoryName(asm.Location);
            _filePath = Path.Combine(_catalogPath, FileName);
        }
        public static int LogSize { get; set; } = 262144; // 256 Kb
        public static string FileName { get; } = "dajet-exchange.log";
        public static string FilePath { get { return _filePath; } }
        public static string CatalogPath { get { return _catalogPath; } }
        public static void Log(string text)
        {
            lock (_syncLog)
            {
                LogSyncronized(text);
            }
        }
        public static void Log(Exception error)
        {
            lock (_syncLog)
            {
                LogSyncronized(GetErrorText(error));
            }
        }
        public static void Log(List<string> errors)
        {
            if (errors == null || errors.Count == 0)
            {
                return;
            }

            lock (_syncLog)
            {
                for (int i = 0; i < errors.Count; i++)
                {
                    LogSyncronized(errors[i]);
                }
            }
        }
        public static void LogSyncronized(string text)
        {
            FileInfo file = new FileInfo(FilePath);

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

            using (StreamWriter writer = new StreamWriter(FilePath, true, Encoding.UTF8))
            {
                writer.WriteLine("[{0}] {1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), text);
            }
        }
        public static string GetErrorText(Exception ex)
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