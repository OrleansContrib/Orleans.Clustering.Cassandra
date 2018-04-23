using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.Extensions.CommandLineUtils;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NuClear.Broadway.Interfaces;
using Orleans;
using Orleans.Configuration;
using Orleans.Hosting;
using Serilog;
using ILogger = Microsoft.Extensions.Logging.ILogger;

namespace NuClear.Broadway.Worker
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var env = Environment.GetEnvironmentVariable("ROADS_ENVIRONMENT") ?? "Production";
            var basePath = Path.GetDirectoryName(Assembly.GetEntryAssembly().Location);
            var configuration = new ConfigurationBuilder()
                .SetBasePath(basePath)
                .AddJsonFile("appsettings.json")
                .AddJsonFile($"appsettings.{env.ToLower()}.json")
                .AddEnvironmentVariables("ROADS_")
                .Build();

            var serilogLogger = CreateLogger(configuration);
            
            var services = new ServiceCollection()
                .AddLogging(x => x.AddSerilog(serilogLogger, true));

            var serviceProvider = services.BuildServiceProvider();
            var loggerFactory = serviceProvider.GetRequiredService<ILoggerFactory>();
            var logger = loggerFactory.CreateLogger<Program>();
            
            var cts = new GrainCancellationTokenSource();
            Console.CancelKeyPress += (sender, eventArgs) =>
            {
                logger.LogInformation("Application is shutting down...");
                cts.Cancel();
                serviceProvider.Dispose();

                eventArgs.Cancel = true;
            };
            var app = new CommandLineApplication { Name = "Broadway.Worker" };
            app.HelpOption(CommandLine.HelpOptionTemplate);
            app.OnExecute(
                () =>
                {
                    Console.WriteLine("Broadway worker host.");
                    app.ShowHelp();
                    return 0;
                });
            
            var clusterClient = CreateClusterClient(configuration, logger, serilogLogger);

            app.Command(
                CommandLine.Commands.Import,
                config =>
                {
                    config.Description = "Run import worker. See available arguments for details.";
                    config.HelpOption(CommandLine.HelpOptionTemplate);
                    config.Command(
                        CommandLine.CommandTypes.Firms,
                        commandConfig =>
                        {
                            commandConfig.Description = "Import firms.";
                            commandConfig.HelpOption(CommandLine.HelpOptionTemplate);
                            commandConfig.OnExecute(() => Run(commandConfig, logger, clusterClient, cts));
                        });
                    config.OnExecute(() =>
                    {
                        config.ShowHelp();
                        return 0;
                    });
                });
            
            var exitCode = 0;
            try
            {
                logger.LogInformation("VStore Worker started with options: {workerOptions}.", args.Length != 0 ? string.Join(" ", args) : "N/A");
                exitCode = app.Execute(args);
            }
            catch (CommandParsingException ex)
            {
                ex.Command.ShowHelp();
                exitCode = 1;
            }
            catch (WorkerNotFoundExeption)
            {
                exitCode = 2;
            }
            catch (Exception ex)
            {
                logger.LogCritical(default, ex, "Unexpected error occured. See logs for details.");
                exitCode = -1;
            }
            finally
            {
                logger.LogInformation("Broadway Worker is shutting down with code {workerExitCode}.", exitCode);
            }

            Environment.Exit(exitCode);
        }
        
        private static Serilog.ILogger CreateLogger(IConfiguration configuration)
        {
            var loggerConfiguration = new LoggerConfiguration().ReadFrom.Configuration(configuration);
            Log.Logger = loggerConfiguration.CreateLogger();

            return Log.Logger;
        }
        
        private static IClusterClient CreateClusterClient(IConfiguration configuration, ILogger logger, Serilog.ILogger serilogLogger)
        {
            const string invariant = "Npgsql";

            var client = new ClientBuilder()
                .Configure<ClusterOptions>(options =>
                {
                    options.ClusterId = "broadway-prototype";
                    options.ServiceId = "broadway";
                })
                .UseAdoNetClustering(options =>
                {
                    options.Invariant = invariant;
                    options.ConnectionString = configuration.GetConnectionString("Orleans");
                })
                .ConfigureApplicationParts(parts => parts.AddApplicationPart(typeof(ICampaignGrain).Assembly).WithReferences())
                .ConfigureLogging(logging => logging.AddSerilog(serilogLogger, true))
                .Build();
            
            StartClientWithRetries(logger, client).Wait();
            
            return client;
        }
        
        private static async Task StartClientWithRetries(
            ILogger logger, 
            IClusterClient client,
            int initializeAttemptsBeforeFailing = 5)
        {
            var attempt = 0;
            while (true)
            {
                try
                {
                    await client.Connect();
                    logger.LogInformation("Client successfully connect to silo host");
                    break;
                }
                catch (Exception ex)
                {
                    attempt++;
                    logger.LogWarning(
                        ex,
                        "Attempt {attempt} of {initializeAttemptsBeforeFailing} failed to initialize the Orleans client.",
                        attempt,
                        initializeAttemptsBeforeFailing);
                    
                    if (attempt > initializeAttemptsBeforeFailing)
                    {
                        return;
                    }

                    await Task.Delay(TimeSpan.FromSeconds(4));
                }
            }
        }

        private static int Run(CommandLineApplication app, ILogger logger, IClusterClient clusterClient, GrainCancellationTokenSource cts)
        {
            var registry = new WorkerGrainRegistry(logger, clusterClient);

            var taskId = app.Parent.Name;
            var taskType = app.Name;
            var workerGrain = registry.GetWorkerGrain(taskId, taskType);

            logger.LogInformation(
                "Starting worker of type {workerType} with identity {workerIndentity}...",
                workerGrain.GetType().Name,
                workerGrain.GetGrainIdentity());
            
            workerGrain.Execute(cts.Token).GetAwaiter().GetResult();
            
            logger.LogInformation(
                "Worker of type {workerType} with identity {workerIndentity} completed successfully.", 
                workerGrain.GetType().Name,
                workerGrain.GetGrainIdentity());

            return 0;
        }
    }
}