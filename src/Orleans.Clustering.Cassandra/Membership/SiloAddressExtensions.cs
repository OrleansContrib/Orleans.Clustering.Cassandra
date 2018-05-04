using Orleans.Runtime;

namespace Orleans.Clustering.Cassandra.Membership
{
    internal static class SiloAddressExtensions
    {
        public static string AsSiloInstanceId(this SiloAddress siloAddress)
            => $"{siloAddress.Endpoint.Address}-{siloAddress.Endpoint.Port}-{siloAddress.Generation}";
    }
}