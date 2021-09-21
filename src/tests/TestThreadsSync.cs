using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace tests
{
    [TestClass] public sealed class TestThreadsSync
    {
        private AutoResetEvent are = new AutoResetEvent(false);

        [TestMethod] public void Test_AutoResetEvent()
        {
            Thread worker = new Thread(WorkerThread);
            worker.IsBackground = true;
            worker.Start();

            int counter = 0;
            while (are.WaitOne(TimeSpan.FromSeconds(2)))
            {
                counter++;
                Console.WriteLine("Main thread: counter = " + counter.ToString());
            }

            Console.WriteLine("Main thread: result = " + counter.ToString());

            Thread.Sleep(TimeSpan.FromSeconds(10));
        }

        private void WorkerThread()
        {
            for (int i = 1; i <= 3; i++)
            {
                Thread.Sleep(TimeSpan.FromSeconds(i));
                Console.WriteLine("Background thread: i = " + i.ToString());
                are.Set();
            }
        }
    }
}