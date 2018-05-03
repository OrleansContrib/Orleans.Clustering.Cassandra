using System;

using Cassandra;

using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Orleans.Clustering.Cassandra.Membership;
using Orleans.Clustering.Cassandra.Options;
using Orleans.Hosting;

namespace Orleans.Clustering.Cassandra
{
    public static class ClusteringExtensions
    {
        public static ISiloHostBuilder UseCassandraClustering(
            this ISiloHostBuilder builder,
            Action<CassandraClusteringOptions> configureOptions,
            ILoggerProvider loggerProvider = null)
        {
            return builder.ConfigureServices(
                services =>
                    {
                        if (configureOptions != null)
                        {
                            services.Configure(configureOptions);
                        }

                        services.AddSingleton<IMembershipTable, CassandraMembershipTable>();

                        Diagnostics.CassandraPerformanceCountersEnabled = true;
                        Diagnostics.CassandraStackTraceIncluded = true;
                        if (loggerProvider != null)
                        {
                            Diagnostics.AddLoggerProvider(loggerProvider);
                        }
                    });
        }
    }
}