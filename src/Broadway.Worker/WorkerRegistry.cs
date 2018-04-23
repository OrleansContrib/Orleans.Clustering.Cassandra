using System;
using System.Collections.Generic;
using System.Reflection;
using Microsoft.Extensions.Logging;
using NuClear.Broadway.Interfaces.Workers;
using Orleans;

namespace NuClear.Broadway.Worker
{
    public class WorkerRegistry
    {
        private static readonly Dictionary<string, Type> Registry =
            new Dictionary<string, Type>
            {
                {"import-firms", typeof(IFirmImportWorkerGrain)}
            };

        private static readonly MethodInfo GetGrainMethodInfo =
            typeof(WorkerRegistry).GetMethod(nameof(GetGrain), BindingFlags.Instance | BindingFlags.NonPublic);
        
        private readonly ILogger _logger;
        private readonly IClusterClient _clusterClient;

        public WorkerRegistry(ILogger logger, IClusterClient clusterClient)
        {
            _logger = logger;
            _clusterClient = clusterClient;
        }

        public IWorkerGrain GetWorker(string taskId, string taskType)
        {
            if (Registry.TryGetValue($"{taskId}-{taskType}", out var workerType))
            {
                var getGrainMethod = GetGrainMethodInfo.MakeGenericMethod(workerType);
                return (IWorkerGrain) getGrainMethod.Invoke(this, Array.Empty<object>());
            }
            
            _logger.LogCritical("Worker for task {taskId} of type {taskType} has not beed registered.", taskId, taskType);
            throw new WorkerNotFoundExeption(taskId, taskType);
        }

        private TGrainInterface GetGrain<TGrainInterface>() where TGrainInterface : IGrainWithIntegerKey
            => _clusterClient.GetGrain<TGrainInterface>(0);
    }
}