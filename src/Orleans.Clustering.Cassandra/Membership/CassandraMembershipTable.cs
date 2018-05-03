using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;

using Cassandra;
using Cassandra.Data.Linq;
using Cassandra.Mapping;

using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Orleans.Clustering.Cassandra.Membership.Models;
using Orleans.Clustering.Cassandra.Options;
using Orleans.Configuration;
using Orleans.Runtime;

namespace Orleans.Clustering.Cassandra.Membership
{
    internal class CassandraMembershipTable : IMembershipTable
    {
        private const ConsistencyLevel ConsistencyLevel = global::Cassandra.ConsistencyLevel.EachQuorum;

        private readonly string _orleansClusterId;
        private readonly IOptions<CassandraClusteringOptions> _cassandraClusteringOptions;
        private readonly ILogger<CassandraMembershipTable> _logger;
        private readonly MappingConfiguration _mappingConfiguration;

        private ISession _session;
        private Table<ClusterMembership> _queryTable;
        private Table<SiloInstance> _siloInstanceTable;
        private Table<ClusterVersion> _clusterVersionTable;

        public CassandraMembershipTable(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<CassandraClusteringOptions> cassandraClusteringOptions,
            ILogger<CassandraMembershipTable> logger)
        {
            _orleansClusterId = clusterOptions.Value.ClusterId;
            _cassandraClusteringOptions = cassandraClusteringOptions;
            _logger = logger;

            _mappingConfiguration = new MappingConfiguration().Define(new EntityMappings(_cassandraClusteringOptions.Value.TableName));
        }

        public async Task InitializeMembershipTable(bool tryInitTableVersion)
        {
            var cassandraOptions = _cassandraClusteringOptions.Value;
            var cluster = Cluster.Builder()
                                 .AddContactPoints(cassandraOptions.ContactPoints)
                                 .WithDefaultKeyspace(cassandraOptions.Keyspace)
                                 .Build();

            _session = cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(
                new Dictionary<string, string>
                    {
                        { "class", "SimpleStrategy" },
                        { "replication_factor", cassandraOptions.ReplicationFactor.ToString() }
                    });

            _queryTable = new Table<ClusterMembership>(_session, _mappingConfiguration);
            await Task.Run(() => _queryTable.CreateIfNotExists());

            _siloInstanceTable = new Table<SiloInstance>(_session, _mappingConfiguration);
            _clusterVersionTable = new Table<ClusterVersion>(_session, _mappingConfiguration);

            if (tryInitTableVersion)
            {
                var insert = _clusterVersionTable.Insert(
                    new ClusterVersion
                        {
                            ClusterId = _orleansClusterId,
                            Timestamp = DateTimeOffset.UtcNow,
                            Version = 0
                        });

                insert.SetConsistencyLevel(ConsistencyLevel);
                await insert.ExecuteAsync();
            }
        }

        public Task DeleteMembershipTableEntries(string clusterId)
        {
            throw new NotImplementedException();
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            try
            {
                var entityId = SiloInstance.ConstructEntityId(key);
                var ids = new[] { entityId, ClusterVersion.Id };
                var data = await _queryTable
                                 .Where(x => x.ClusterId == _orleansClusterId && ids.Contains(x.EntityId))
                                 .AllowFiltering()
                                 .SetConsistencyLevel(ConsistencyLevel)
                                 .ExecuteAsync();

                return CreateMembershipTableData(data);
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Unexpected error occured while reading data for silo with key {siloKey}.", key.ToString());
                throw;
            }
        }

        public async Task<MembershipTableData> ReadAll()
        {
            try
            {
                var data = await _queryTable
                                 .Where(x => x.ClusterId == _orleansClusterId)
                                 .AllowFiltering()
                                 .SetConsistencyLevel(ConsistencyLevel)
                                 .ExecuteAsync();

                return CreateMembershipTableData(data);
            }
            catch (Exception ex)
            {
                _logger.LogCritical(ex, "Unexpected error occured while reading all cluster membership data.");
                throw;
            }
        }

        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            var siloInstance = SiloInstance.FromMembershipEntry(_orleansClusterId, entry);
            var clusterVersion = ClusterVersion.FromTableVersion(_orleansClusterId, tableVersion);

            var mapper = new Mapper(_session, _mappingConfiguration);

            var batch = mapper.CreateBatch().WithOptions(x => x.SetConsistencyLevel(ConsistencyLevel));
            batch.Insert(siloInstance);
            batch.Update(clusterVersion);

            await mapper.ExecuteAsync(batch);

            return true;
        }

        public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            var siloInstance = SiloInstance.FromMembershipEntry(_orleansClusterId, entry);
            var clusterVersion = ClusterVersion.FromTableVersion(_orleansClusterId, tableVersion);

            var mapper = new Mapper(_session, _mappingConfiguration);

            var batch = mapper.CreateBatch().WithOptions(x => x.SetConsistencyLevel(ConsistencyLevel));
            batch.Update(siloInstance);
            batch.Update(clusterVersion);

            await mapper.ExecuteAsync(batch);

            return true;
        }

        public async Task UpdateIAmAlive(MembershipEntry entry)
        {
            var siloInstance = SiloInstance.FromMembershipEntry(_orleansClusterId, entry);
            var mapper = new Mapper(_session, _mappingConfiguration);
            await Task.Run(() => mapper.Update(siloInstance, CqlQueryOptions.New().SetConsistencyLevel(ConsistencyLevel)));
        }

        private static MembershipTableData CreateMembershipTableData(IEnumerable<ClusterMembership> data)
        {
            TableVersion tableVersion = null;
            var members = new List<Tuple<MembershipEntry, string>>();
            foreach (var item in data)
            {
                if (item.EntityId == ClusterVersion.Id)
                {
                    tableVersion = new TableVersion(item.Version, string.Empty);
                }
                else
                {
                    var entry = new MembershipEntry
                        {
                            SiloName = item.SiloName,
                            HostName = item.HostName,
                            Status = (SiloStatus)item.Status,
                            RoleName = item.RoleName,
                            UpdateZone = item.UpdateZone,
                            FaultZone = item.FaultZone,
                            StartTime = item.StartTime.UtcDateTime,
                            IAmAliveTime = item.IAmAliveTime.UtcDateTime,
                            SiloAddress = SiloAddress.New(new IPEndPoint(IPAddress.Parse(item.Address), item.Port), item.Generation)
                        };

                    if (item.ProxyPort.HasValue)
                    {
                        entry.ProxyPort = item.ProxyPort.Value;
                    }

                    var suspectingSilos = new List<SiloAddress>();
                    var suspectingTimes = new List<DateTime>();

                    foreach (var silo in item.SuspectingSilos)
                    {
                        suspectingSilos.Add(SiloAddress.FromParsableString(silo));
                    }

                    foreach (var time in item.SuspectingTimes)
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

                    members.Add(Tuple.Create(entry, string.Empty));
                }
            }

            return new MembershipTableData(members, tableVersion);
        }
    }
}