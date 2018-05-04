using System;
using System.Collections.Generic;
using System.Linq;
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

        private Mapper _mapper;
        private Table<ClusterMembership> _dataTable;

        public CassandraMembershipTable(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<CassandraClusteringOptions> cassandraClusteringOptions,
            ILogger<CassandraMembershipTable> logger)
        {
            _orleansClusterId = clusterOptions.Value.ClusterId;
            _cassandraClusteringOptions = cassandraClusteringOptions;
            _logger = logger;
        }

        public async Task InitializeMembershipTable(bool tryInitTableVersion)
        {
            try
            {
                var cassandraOptions = _cassandraClusteringOptions.Value;
                var cluster = Cluster.Builder()
                                     .AddContactPoints(cassandraOptions.ContactPoints)
                                     .WithDefaultKeyspace(cassandraOptions.Keyspace)
                                     .Build();

                var session = cluster.ConnectAndCreateDefaultKeyspaceIfNotExists(
                    new Dictionary<string, string>
                        {
                            { "class", "SimpleStrategy" },
                            { "replication_factor", cassandraOptions.ReplicationFactor.ToString() }
                        });

                var mappingConfiguration = new MappingConfiguration().Define(new EntityMappings(_cassandraClusteringOptions.Value.TableName));

                _dataTable = new Table<ClusterMembership>(session, mappingConfiguration);
                await Task.Run(() => _dataTable.CreateIfNotExists());

                _mapper = new Mapper(session, mappingConfiguration);

                if (tryInitTableVersion)
                {
                    await _mapper.InsertAsync(
                        ClusterVersion.New(_orleansClusterId),
                        CqlQueryOptions.New().SetConsistencyLevel(ConsistencyLevel));
                }
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while initializing membership data table for cluster {clusterId}.", _orleansClusterId);
                throw;
            }
        }

        public async Task DeleteMembershipTableEntries(string clusterId)
        {
            try
            {
                var data = await _dataTable
                                 .Where(x => x.ClusterId == _orleansClusterId)
                                 .AllowFiltering()
                                 .SetConsistencyLevel(ConsistencyLevel)
                                 .ExecuteAsync();

                var batch = _mapper.CreateBatch().WithOptions(x => x.SetConsistencyLevel(ConsistencyLevel));
                foreach (var item in data)
                {
                    batch.Delete(item);
                }

                await _mapper.ExecuteAsync(batch);
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while deleting membership data for cluster {clusterId}.", clusterId);
                throw;
            }
        }

        public async Task<MembershipTableData> ReadRow(SiloAddress key)
        {
            try
            {
                var entityId = key.AsSiloInstanceId();
                var ids = new[] { entityId, ClusterVersion.Id };
                var data = await _dataTable
                                 .Where(x => x.ClusterId == _orleansClusterId && ids.Contains(x.EntityId))
                                 .AllowFiltering()
                                 .SetConsistencyLevel(ConsistencyLevel)
                                 .ExecuteAsync();

                return CreateMembershipTableData(data);
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while reading data for silo with key {siloKey}.", key.ToString());
                throw;
            }
        }

        public async Task<MembershipTableData> ReadAll()
        {
            try
            {
                var data = await _dataTable
                                 .Where(x => x.ClusterId == _orleansClusterId)
                                 .AllowFiltering()
                                 .SetConsistencyLevel(ConsistencyLevel)
                                 .ExecuteAsync();

                return CreateMembershipTableData(data);
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while reading all cluster membership data.");
                throw;
            }
        }

        public async Task<bool> InsertRow(MembershipEntry entry, TableVersion tableVersion)
        {
            try
            {
                var siloInstance = entry.AsSiloInstance(_orleansClusterId);
                var clusterVersion = tableVersion.AsClusterVersion(_orleansClusterId);

                var batch = _mapper.CreateBatch().WithOptions(x => x.SetConsistencyLevel(ConsistencyLevel));
                batch.Insert(siloInstance);
                batch.Update(clusterVersion);

                await _mapper.ExecuteAsync(batch);

                return true;
            }
            catch (DriverException)
            {
                _logger.LogWarning(
                    "Cassandra driver error occured while inserting row for silo {silo}, cluster version = {clusterVersion}.",
                    entry.ToString(),
                    tableVersion.Version);
                throw;
            }
        }

        public async Task<bool> UpdateRow(MembershipEntry entry, string etag, TableVersion tableVersion)
        {
            try
            {
                var siloInstance = entry.AsSiloInstance(_orleansClusterId);
                var clusterVersion = tableVersion.AsClusterVersion(_orleansClusterId);

                var batch = _mapper.CreateBatch().WithOptions(x => x.SetConsistencyLevel(ConsistencyLevel));
                batch.Update(siloInstance);
                batch.Update(clusterVersion);

                await _mapper.ExecuteAsync(batch);

                return true;
            }
            catch (DriverException)
            {
                _logger.LogWarning(
                    "Cassandra driver error occured while updating row for silo {silo}, cluster version = {clusterVersion}.",
                    entry.ToString(),
                    tableVersion.Version);
                throw;
            }
        }

        public async Task UpdateIAmAlive(MembershipEntry entry)
        {
            try
            {
                var siloInstance = entry.AsSiloInstance(_orleansClusterId);
                await _mapper.UpdateAsync(siloInstance, CqlQueryOptions.New().SetConsistencyLevel(ConsistencyLevel));
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while updating liveness status for silo {silo}.", entry.ToString());
                throw;
            }
        }

        private static MembershipTableData CreateMembershipTableData(IEnumerable<ClusterMembership> data)
        {
            TableVersion tableVersion = null;
            var members = new List<Tuple<MembershipEntry, string>>();
            foreach (var item in data)
            {
                if (item.EntityId == ClusterVersion.Id)
                {
                    tableVersion = item.AsTableVersion();
                }
                else
                {
                    var entry = item.AsMembershipEntry();
                    members.Add(Tuple.Create(entry, string.Empty));
                }
            }

            return new MembershipTableData(members, tableVersion);
        }
    }
}