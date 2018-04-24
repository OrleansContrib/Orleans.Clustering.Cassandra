using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.Extensions.Logging;
using NuClear.Broadway.Interfaces;
using NuClear.Broadway.Interfaces.Workers;
using Orleans;

namespace NuClear.Broadway.Worker
{
    public class WorkerGrainRegistry
    {
        private static readonly Dictionary<string, Type> Registry =
            new Dictionary<string, Type>
            {
                {"import-firms", typeof(IFirmImportWorkerGrain)},
                {"import-flow-kaleidoscope", typeof(IFlowKaleidoscopeConsumerGrain)}
            };

        private static readonly MethodInfo GetGrainMethodInfo =
            typeof(WorkerGrainRegistry).GetMethod(nameof(GetGrain), BindingFlags.Instance | BindingFlags.NonPublic);
        
        private readonly ILogger _logger;
        private readonly IClusterClient _clusterClient;

        public WorkerGrainRegistry(ILogger logger, IClusterClient clusterClient)
        {
            _logger = logger;
            _clusterClient = clusterClient;
        }

        public IWorkerGrain GetWorkerGrain(string taskId, string taskType)
        {
            var key = $"{taskId}-{taskType}";
            if (Registry.TryGetValue(key, out var workerType))
            {
                var getGrainMethod = GetGrainMethodInfo.MakeGenericMethod(workerType);
                return (IWorkerGrain) getGrainMethod.Invoke(this, new object[] {key});
            }
            
            _logger.LogCritical("Worker for task {taskId} of type {taskType} has not beed registered.", taskId, taskType);
            throw new WorkerNotFoundExeption(taskId, taskType);
        }

        private TWorkerGrain GetGrain<TWorkerGrain>(string key) where TWorkerGrain : IWorkerGrain
            => _clusterClient.GetGrain<TWorkerGrain>(key);
    }
}