using System;

namespace NuClear.Broadway.Worker
{
    public sealed class WorkerNotFoundExeption : Exception
    {
        public WorkerNotFoundExeption(string taskId, string taskType)
            : base($"Worker for task '{taskId}' of type '{taskType}' not found.")
        {
        }
    }
}