using System;
using System.Collections.Generic;
using System.Net;

using Orleans.Runtime;

namespace Orleans.Clustering.Cassandra.Membership.Models
{
    internal static class EntityExtensions
    {
        public static TableVersion AsTableVersion(this IClusterVersion clusterVersion)
            => new TableVersion(clusterVersion.Version, string.Empty);

        public static ClusterVersion AsClusterVersion(this TableVersion tableVersion, string clusterId)
            => new ClusterVersion
                {
                    ClusterId = clusterId,
                    Timestamp = DateTimeOffset.UtcNow,
                    Version = tableVersion.Version
                };

        public static MembershipEntry AsMembershipEntry(this ISiloIntance siloIntance)
        {
            var entry = new MembershipEntry
                {
                    SiloName = siloIntance.SiloName,
                    HostName = siloIntance.HostName,
                    Status = (SiloStatus)siloIntance.Status,
                    RoleName = siloIntance.RoleName,
                    UpdateZone = siloIntance.UpdateZone,
                    FaultZone = siloIntance.FaultZone,
                    StartTime = siloIntance.StartTime.UtcDateTime,
                    IAmAliveTime = siloIntance.IAmAliveTime.UtcDateTime,
                    SiloAddress = SiloAddress.New(new IPEndPoint(IPAddress.Parse(siloIntance.Address), siloIntance.Port), siloIntance.Generation)
                };

            if (siloIntance.ProxyPort.HasValue)
            {
                entry.ProxyPort = siloIntance.ProxyPort.Value;
            }

            var suspectingSilos = new List<SiloAddress>();
            var suspectingTimes = new List<DateTime>();

            foreach (var silo in siloIntance.SuspectingSilos)
            {
                suspectingSilos.Add(SiloAddress.FromParsableString(silo));
            }

            foreach (var time in siloIntance.SuspectingTimes)
            {
                suspectingTimes.Add(time.UtcDateTime);
            }

            if (suspectingSilos.Count != suspectingTimes.Count)
            {
                throw new OrleansException(
                    $"SuspectingSilos.Length of {suspectingSilos.Count} as read from Cassandra " +
                    $"is not equal to SuspectingTimes.Length of {suspectingTimes.Count}");
            }

            for (var i = 0; i < suspectingSilos.Count; i++)
            {
                entry.AddSuspector(suspectingSilos[i], suspectingTimes[i]);
            }

            return entry;
        }

        public static SiloInstance AsSiloInstance(this MembershipEntry entry, string clusterId)
        {
            var siloInstance = new SiloInstance
                {
                    ClusterId = clusterId,
                    EntityId = entry.SiloAddress.AsSiloInstanceId(),
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