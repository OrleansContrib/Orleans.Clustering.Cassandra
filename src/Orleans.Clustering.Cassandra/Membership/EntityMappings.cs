using Cassandra.Mapping;

using Orleans.Clustering.Cassandra.Membership.Models;

namespace Orleans.Clustering.Cassandra.Membership
{
    public class EntityMappings : Mappings
    {
        internal EntityMappings(string tableName)
        {
            For<ClusterMembership>()
                .TableName(tableName)
                .PartitionKey(x => x.ClusterId, x => x.EntityId);

            For<SiloInstance>()
                .TableName(tableName)
                .PartitionKey(x => x.ClusterId, x => x.EntityId);

            For<ClusterVersion>()
                .TableName(tableName)
                .PartitionKey(x => x.ClusterId, x => x.EntityId);
        }
    }
}