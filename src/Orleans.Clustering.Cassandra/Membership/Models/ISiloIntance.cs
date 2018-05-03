using System;
using System.Collections.Generic;

namespace Orleans.Clustering.Cassandra.Membership.Models
{
    internal interface ISiloIntance : IEntityBase
    {
        string Address { get; set; }
        int Port { get; set; }
        int Generation { get; set; }

        string SiloName { get; set; }
        string HostName { get; set; }
        int Status { get; set; }
        int? ProxyPort { get; set; }

        string RoleName { get; set; }
        int UpdateZone { get; set; }
        int FaultZone { get; set; }

        List<string> SuspectingSilos { get; set; }
        List<DateTimeOffset> SuspectingTimes { get; set; }

        DateTimeOffset StartTime { get; set; }
        DateTimeOffset IAmAliveTime { get; set; }
    }
}