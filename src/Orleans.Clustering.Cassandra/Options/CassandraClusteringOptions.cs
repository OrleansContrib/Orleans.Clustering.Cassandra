using System.Collections.Generic;

namespace Orleans.Clustering.Cassandra.Options
{
    public class CassandraClusteringOptions
    {
        public IEnumerable<string> ContactPoints { get; set; }
        public string Keyspace { get; set; } = "orleans";
        public string TableName { get; set; } = "cluster_membership";
        public int ReplicationFactor { get; set; } = 3;

        public DiagnosticsOptions Diagnostics { get; set; } = new DiagnosticsOptions();

        public class DiagnosticsOptions
        {
            public bool PerformanceCountersEnabled { get; set; }
            public bool StackTraceIncluded { get; set; }
        }
    }
}