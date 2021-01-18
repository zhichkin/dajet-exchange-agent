using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DaJet.RabbitMQ.Producer
{
    public static class Program
    {
        static void Main(string[] args)
        {
            CreateHostBuilder(args).Build().Run();
        }
        public static IHostBuilder CreateHostBuilder(string[] args)
        {
            return Host.CreateDefaultBuilder(args)
                // .UseWindowsService() from Microsoft.Extensions.Hosting.WindowsServices package
                // https://csharp.christiannagel.com/2019/10/15/windowsservice/
                .ConfigureServices((context, services) =>
                {
                    services.AddHostedService<Worker>();
                });
        }
    }
}