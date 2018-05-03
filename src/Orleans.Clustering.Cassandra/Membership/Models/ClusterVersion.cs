using System;

namespace Orleans.Clustering.Cassandra.Membership.Models
{
    internal sealed class ClusterVersion : IClusterVersion
    {
        public const string Id = nameof(ClusterVersion);

        public string EntityId { get; set; } = Id;
        public string EntityType { get; set; } = Id;

        public string ClusterId { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public int Version { get; set; }

        public static ClusterVersion FromTableVersion(string clusterId, TableVersion tableVersion)
            => new ClusterVersion
                {
                    ClusterId = clusterId,
                    Timestamp = DateTimeOffset.UtcNow,
                    Version = tableVersion.Version
                };
    }
}