using System;

namespace Orleans.Clustering.Cassandra.Membership.Models
{
    internal sealed class ClusterVersion : IClusterVersion
    {
        public const string Id = nameof(ClusterVersion);
        public const string Type = Id;

        public string EntityId { get; set; } = Id;
        public string EntityType { get; set; } = Type;

        public string ClusterId { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public int Version { get; set; }

        public static ClusterVersion New(string clusterId)
            => new ClusterVersion
                {
                    ClusterId = clusterId,
                    Timestamp = DateTimeOffset.UtcNow,
                    Version = 0
                };
    }
}