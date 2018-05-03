using System;
using System.Collections.Generic;

namespace Orleans.Clustering.Cassandra.Membership.Models
{
    internal sealed class ClusterMembership : ISiloIntance, IClusterVersion
    {
        public string EntityId { get; set; }
        public string EntityType { get; set; }
        public string ClusterId { get; set; }
        public string Address { get; set; }
        public int Port { get; set; }
        public int Generation { get; set; }
        public string SiloName { get; set; }
        public string HostName { get; set; }
        public int Status { get; set; }
        public int? ProxyPort { get; set; }
        public string RoleName { get; set; }
        public int UpdateZone { get; set; }
        public int FaultZone { get; set; }
        public List<string> SuspectingSilos { get; set; }
        public List<DateTimeOffset> SuspectingTimes { get; set; }
        public DateTimeOffset StartTime { get; set; }
        public DateTimeOffset IAmAliveTime { get; set; }
        public DateTimeOffset Timestamp { get; set; }
        public int Version { get; set; }
    }
}