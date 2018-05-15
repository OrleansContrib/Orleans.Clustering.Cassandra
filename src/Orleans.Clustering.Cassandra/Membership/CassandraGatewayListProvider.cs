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
using Orleans.Messaging;
using Orleans.Runtime;

namespace Orleans.Clustering.Cassandra.Membership
{
    internal sealed class CassandraGatewayListProvider : IGatewayListProvider
    {
        private const ConsistencyLevel DefaultConsistencyLevel = ConsistencyLevel.EachQuorum;

        private readonly string _clusterId;
        private readonly CassandraClusteringOptions _cassandraClusteringOptions;
        private readonly ILogger<CassandraGatewayListProvider> _logger;

        private Table<ClusterMembership> _dataTable;

        public CassandraGatewayListProvider(
            IOptions<ClusterOptions> clusterOptions,
            IOptions<GatewayOptions> gatewayOptions,
            IOptions<CassandraClusteringOptions> cassandraClusteringOptions,
            ILogger<CassandraGatewayListProvider> logger,
            ILoggerProvider loggerProvider)
        {
            _clusterId = clusterOptions.Value.ClusterId;
            _cassandraClusteringOptions = cassandraClusteringOptions.Value;
            _logger = logger;
            MaxStaleness = gatewayOptions.Value.GatewayListRefreshPeriod;

            Diagnostics.CassandraPerformanceCountersEnabled = _cassandraClusteringOptions.Diagnostics.PerformanceCountersEnabled;
            Diagnostics.CassandraStackTraceIncluded = _cassandraClusteringOptions.Diagnostics.StackTraceIncluded;

            if (loggerProvider != null)
            {
                Diagnostics.AddLoggerProvider(loggerProvider);
            }
        }

        public TimeSpan MaxStaleness { get; }
        public bool IsUpdatable => true;

        public async Task InitializeGatewayListProvider()
        {
            try
            {
                var cassandraCluster =
                    Cluster.Builder()
                           .AddContactPoints(_cassandraClusteringOptions.ContactPoints.Split(','))
                           .WithDefaultKeyspace(_cassandraClusteringOptions.Keyspace)
                           .Build();

                var session = await cassandraCluster.ConnectAsync();
                var mappingConfiguration = new MappingConfiguration().Define(new EntityMappings(_cassandraClusteringOptions.TableName));
                _dataTable = new Table<ClusterMembership>(session, mappingConfiguration);
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while initializing gateway list provider for cluster {clusterId}.", _clusterId);
                throw;
            }
        }

        public async Task<IList<Uri>> GetGateways()
        {
            try
            {
                var data = await _dataTable
                                 .Where(
                                     x => x.ClusterId == _clusterId &&
                                          x.EntityType == SiloInstance.Type &&
                                          x.Status == (int)SiloStatus.Active &&
                                          x.ProxyPort > 0)
                                 .AllowFiltering()
                                 .SetConsistencyLevel(DefaultConsistencyLevel)
                                 .ExecuteAsync();

                return data.Select(ConvertToGatewayUri).ToList();
            }
            catch (DriverException)
            {
                _logger.LogWarning("Cassandra driver error occured while getting gateway list for cluster {clusterId}.", _clusterId);
                throw;
            }
        }

        private static Uri ConvertToGatewayUri(ISiloIntance gateway)
        {
            // ReSharper disable once PossibleInvalidOperationException
            var address = SiloAddress.New(new IPEndPoint(IPAddress.Parse(gateway.Address), gateway.ProxyPort.Value), gateway.Generation);
            return address.ToGatewayUri();
        }
    }
}