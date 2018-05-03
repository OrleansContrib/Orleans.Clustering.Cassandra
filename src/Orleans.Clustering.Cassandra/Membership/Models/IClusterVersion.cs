using System;

namespace Orleans.Clustering.Cassandra.Membership.Models
{
    internal interface IClusterVersion : IEntityBase
    {
        DateTimeOffset Timestamp { get; set; }
        int Version { get; set; }
    }
}