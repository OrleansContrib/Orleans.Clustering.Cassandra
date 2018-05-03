namespace Orleans.Clustering.Cassandra.Membership.Models
{
    internal interface IEntityBase
    {
        string EntityId { get; set; }
        string EntityType { get; set; }
        string ClusterId { get; set; }
    }
}