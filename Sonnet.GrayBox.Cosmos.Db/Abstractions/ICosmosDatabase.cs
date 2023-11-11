using Microsoft.Azure.Cosmos;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Sonnet.GrayBox.Cosmos.Db.Abstractions
{
    public interface ICosmosDatabase<T>
    {
        string Name { get; set; }
        string EndpointUri { get; set; }
        string PrimaryKey { get; set; }
        double RequestCharge { get; }

        string DatabaseId { get; }
        string ContainerId { get; }


        Task InitAsync(string databaseId, string containerId);

        /// <summary>
        /// Call when your Cosmos database and container already exist
        /// </summary>
        /// <param name="databaseId"></param>
        /// <param name="containerId"></param>
        void Load(string databaseId, string containerId);
        Task CreateItemAsync(T item, string id, PartitionKey partitionKey);
        Task<T> ReadItemAsync(string id, string partitionKey);
        Task<List<dynamic>> QueryItemDynamicStreamAsync(string query);
        Task<List<T>> QueryItemAsync(string query);
        Task<T> UpdateItemAsync(T item, string id, PartitionKey partitionKey);
        Task DeleteItemAsync(string id, string partitionKey);
    }
}
