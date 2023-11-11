﻿using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Sonnet.GrayBox.Cosmos.Db.Abstractions;
using Sonnet.GrayBox.Cosmos.Db.Common;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;

namespace Sonnet.GrayBox.Cosmos.Db.Connect
{
    public class CosmosDatabase<T> : ICosmosDatabase<T>
    {
        private Database _database;
        private Container _container;
        private CosmosClient _cosmosClient;
        private readonly ILogger<CosmosDatabase<T>> _logger;

        public string EndpointUri { get; set; }
        public string PrimaryKey { get; set; }
        public string Name { get; set; }
        public string PartitionKeyPath { get; set; }
        public int? ThroughPut { get; set; }
        public ConnectionMode ConnectionMode { get; set; }

        public string DatabaseId { get; private set; }
        public string ContainerId { get; private set; }
        /// <summary>
        /// Will contain total amount of RUs consumed for all request for lifetime of the object
        /// </summary>
        public double RequestCharge { get; private set; }
        /// <summary>
        /// Will contain total amount of seconds for all request for lifetime of the object
        /// </summary>
        public double RequestTimeInSeconds { get; private set; }

        public CosmosDatabase()
        {
            this.ConnectionMode = ConnectionMode.Direct;
            this.PartitionKeyPath = "/pk";
            this.ThroughPut = 400;
        }

        public CosmosDatabase(ILogger<CosmosDatabase<T>> logger)
        {
            this._logger = logger;
            this.ConnectionMode = ConnectionMode.Direct;
            this.PartitionKeyPath = "/pk";
            this.ThroughPut = 400;
        }

        public async Task InitAsync(string databaseId, string containerId)
        {
            CheckSettings(databaseId, containerId);

            // Create a new instance of the Cosmos Client
            this._cosmosClient = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions() { ApplicationName = Name, ConnectionMode = this.ConnectionMode });
            this._database = await this._cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            this._container = await this._database.CreateContainerIfNotExistsAsync(containerId, this.PartitionKeyPath, this.ThroughPut);

            this.DatabaseId = databaseId;
            this.ContainerId = containerId;
        }

        public void Load(string databaseId, string containerId)
        {
            CheckSettings(databaseId, containerId);

            // Create a new instance of the Cosmos Client
            this._cosmosClient = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions() { ApplicationName = Name, ConnectionMode = this.ConnectionMode });
            this._database = _cosmosClient.GetDatabase(databaseId);
            this._container = _database.GetContainer(containerId);
            

            this.DatabaseId = databaseId;
            this.ContainerId = containerId;
        }

        public async Task CreateItemAsync(T item, string id, PartitionKey partitionKey)
        {
            DateTime start = DateTime.Now;

            try
            {
                CheckContainer();
                ItemResponse<T> topicResponse = await this._container.ReadItemAsync<T>(id: id, partitionKey: partitionKey);
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                ItemResponse<T> itemResponse = await this._container.CreateItemAsync<T>(item: item, partitionKey: partitionKey);
                //see the amount of RUs consumed on this request
                this.RequestCharge += itemResponse.RequestCharge;
            }
            catch (CosmosException e)
            {
                _logger?.LogError($"Status Code: { e.StatusCode } | Message: { e.Message }");
                throw e;
            }
            catch (Exception e)
            {
                _logger?.LogError($"Exception: { e.Message }");
                throw e;
            }
            finally
            {
                this.RequestTimeInSeconds += (DateTime.Now - start).TotalSeconds;
            }
        } 

        public async Task<T> ReadItemAsync(string id, string partitionKey)
        {
            DateTime start = DateTime.Now;
            ItemResponse<T> response;

            try
            {
                CheckContainer();

                response = await _container.ReadItemAsync<T>(id: id, partitionKey: new PartitionKey(partitionKey));
                //see the amount of RUs consumed on this request
                this.RequestCharge += response.RequestCharge;
            }
            catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                _logger?.LogError($"Status Code: { e.StatusCode } | Message: { e.Message }");
                response = null;
            }
            catch (CosmosException e)
            {
                _logger?.LogError($"Status Code: { e.StatusCode } | Message: { e.Message }");
                throw e;
            }
            catch (Exception e)
            {
                _logger?.LogError($"Exception: { e.Message }");
                throw e;
            }
            finally
            {
                this.RequestTimeInSeconds += (DateTime.Now - start).TotalSeconds;
            }

            return response ?? Task.FromResult<T>(default).Result;
        }

        public async Task<List<dynamic>> QueryItemDynamicStreamAsync(string query)
        {
            List<dynamic> result = new List<dynamic>();
            DateTime start = DateTime.Now;

            try
            {
                CheckContainer();

                QueryDefinition queryDefinition = new QueryDefinition(query);
                JsonSerializer jsonSerializer = new JsonSerializer();

                using FeedIterator iterator = this._container.GetItemQueryStreamIterator(queryDefinition);
                while (iterator.HasMoreResults)
                {
                    using ResponseMessage response = await iterator.ReadNextAsync();
                    response.EnsureSuccessStatusCode();

                    //see the amount of RUs consumed on this request
                    this.RequestCharge += response.Headers.RequestCharge;

                    using StreamReader sr = new StreamReader(response.Content);
                    using JsonTextReader jtr = new JsonTextReader(sr);
                    dynamic jAr = jsonSerializer.Deserialize<dynamic>(jtr).Documents;

                    for (int i = 0, max = jAr.Count; i < max; i++)
                        result.Add(jAr[i]);
                }
            }
            catch (CosmosException e)
            {
                _logger?.LogError($"Status Code: { e.StatusCode } | Message: { e.Message }");
                throw e;
            }
            catch (Exception e)
            {
                _logger?.LogError($"Exception: { e.Message }");
                throw e;
            }
            finally
            {
                this.RequestTimeInSeconds += (DateTime.Now - start).TotalSeconds;
            }

            return result;
        }

        public async Task<List<T>> QueryItemAsync(string query)
        {
            List<T> result = new List<T>();
            DateTime start = DateTime.Now;

            try
            {
                CheckContainer();

                QueryDefinition queryDefinition = new QueryDefinition(query);
                using FeedIterator<T> iterator = this._container.GetItemQueryIterator<T>(queryDefinition: queryDefinition);
                while (iterator.HasMoreResults)
                {
                    FeedResponse<T> resultSet = await iterator.ReadNextAsync();
                    //see the amount of RUs consumed on this request
                    this.RequestCharge += resultSet.RequestCharge;

                    foreach (T item in resultSet)
                    {
                        result.Add(item);
                    }
                }
            }
            catch (CosmosException e)
            {
                _logger?.LogError($"Status Code: { e.StatusCode } | Message: { e.Message }");
                throw e;
            }
            catch (Exception e)
            {
                _logger?.LogError($"Exception: { e.Message }");
                throw e;
            }
            finally
            {
                this.RequestTimeInSeconds += (DateTime.Now - start).TotalSeconds;
            }

            return result;
        }

        public async Task<T> UpdateItemAsync(T item, string id, PartitionKey partitionKey)
        {
            ItemResponse<T> response;
            DateTime start = DateTime.Now;

            try
            {
                CheckContainer();

                response = await this._container.ReplaceItemAsync<T>(item, id, partitionKey);
                //see the amount of RUs consumed on this request
                this.RequestCharge += response.RequestCharge;
            }
            catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                _logger?.LogError($"Status Code: { e.StatusCode } | Message: { e.Message }");
                response = null;
            }
            catch (CosmosException e)
            {
                _logger?.LogError($"Status Code: { e.StatusCode } | Message: { e.Message }");
                throw e;
            }
            catch (Exception e)
            {
                _logger?.LogError($"Exception: { e.Message }");
                throw e;
            }
            finally
            {
                this.RequestTimeInSeconds += (DateTime.Now - start).TotalSeconds;
            }
            return response ?? Task.FromResult<T>(default).Result;
        }

        public async Task DeleteItemAsync(string id, string partitionKey)
        {
            ItemResponse<T> response;
            DateTime start = DateTime.Now;

            try
            {
                CheckContainer();

                response = await _container.DeleteItemAsync<T>(partitionKey: new PartitionKey(partitionKey), id: id);
                //see the amount of RUs consumed on this request
                this.RequestCharge += response.RequestCharge;
            }
            catch (CosmosException e) when (e.StatusCode == HttpStatusCode.NotFound)
            {
                _logger?.LogError($"Status Code: { e.StatusCode } | Message: { e.Message }");
                response = null;
            }
            catch (CosmosException e)
            {
                _logger?.LogError($"Status Code: { e.StatusCode } | Message: { e.Message }");
                throw e;
            }
            catch (Exception e)
            {
                _logger?.LogError($"Exception: { e.Message }");
                throw e;
            }
            finally
            {
                this.RequestTimeInSeconds += (DateTime.Now - start).TotalSeconds;
            }
        }

        private void CheckSettings(string databaseId = null, string containerId = null)
        {
            if (string.IsNullOrEmpty(EndpointUri) 
                || string.IsNullOrEmpty(PrimaryKey) 
                || string.IsNullOrEmpty(Name) 
                || string.IsNullOrEmpty(databaseId ?? DatabaseId) 
                || string.IsNullOrEmpty(containerId ?? ContainerId))
            {
                _logger.LogCritical(CosmosConnectException.DbInfoOmitted);
                throw new CosmosConnectException(CosmosConnectException.DbInfoOmitted);
            }
        }

        private void CheckContainer()
        {
            if (_container == null)
            {
                _logger.LogCritical(CosmosConnectException.UninitializedContainer);
                throw new CosmosConnectException(CosmosConnectException.UninitializedContainer);
            }
        }
    }
}
