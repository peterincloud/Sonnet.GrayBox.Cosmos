using System;
using System.Collections.Generic;
using System.Text;

namespace Sonnet.GrayBox.Cosmos.Db.Common
{
    public class CosmosConnectException : Exception
    {
        public const string DbInfoOmitted = "EndpointUri, PrimaryKey and database Name properties not initialized";
        public const string UninitializedContainer = "Cosmos Container not initialized. Call Init()";
        public const string ArgumentException = "One or more arguments are invalid";
        public CosmosConnectException(string message) : base(message) { }
    }
}
