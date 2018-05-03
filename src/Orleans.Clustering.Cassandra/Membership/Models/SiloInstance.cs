using System;
using System.Collections.Generic;

using Orleans.Runtime;

namespace Orleans.Clustering.Cassandra.Membership.Models
{
    internal sealed class SiloInstance : ISiloIntance
    {
        public string EntityId { get; set; }
        public string EntityType { get; set; } = nameof(SiloInstance);

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

        public static string ConstructEntityId(SiloAddress siloAddress)
            => $"{siloAddress.Endpoint.Address}-{siloAddress.Endpoint.Port}-{siloAddress.Generation}";

        public static SiloInstance FromMembershipEntry(string clusterId, MembershipEntry entry)
        {
            var siloInstance = new SiloInstance
                {
                    ClusterId = clusterId,
                    EntityId = SiloInstance.ConstructEntityId(entry.SiloAddress),
                    Address = entry.SiloAddress.Endpoint.Address.ToString(),
                    Port = entry.SiloAddress.Endpoint.Port,
                    Generation = entry.SiloAddress.Generation,
                    SiloName = entry.SiloName,
                    HostName = entry.HostName,
                    Status = (int)entry.Status,
                    ProxyPort = entry.ProxyPort,
                    RoleName = entry.RoleName,
                    UpdateZone = entry.UpdateZone,
                    FaultZone = entry.FaultZone,
                    StartTime = entry.StartTime.ToUniversalTime(),
                    IAmAliveTime = entry.IAmAliveTime.ToUniversalTime()
                };

            if (entry.SuspectTimes != null)
            {
                if (siloInstance.SuspectingSilos == null)
                {
                    siloInstance.SuspectingSilos = new List<string>();
                }

                if (siloInstance.SuspectingTimes == null)
                {
                    siloInstance.SuspectingTimes = new List<DateTimeOffset>();
                }

                foreach (var tuple in entry.SuspectTimes)
                {
                    siloInstance.SuspectingSilos.Add(tuple.Item1.ToParsableString());
                    siloInstance.SuspectingTimes.Add(tuple.Item2);
                }
            }

            return siloInstance;
        }
    }
}