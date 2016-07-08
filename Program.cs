using Amazon;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using System;
using System.Collections.Generic;
using System.Configuration;

namespace TDC2016_UtilizandoDynamoDBCSharp
{
    class Program
    {
        static void Main(string[] args)
        {
            while (true)
            {
                System.Console.Clear();
                System.Console.WriteLine("1 - Create Table");
                System.Console.WriteLine("2 - Save Item");
                System.Console.WriteLine("3 - Count");
                System.Console.WriteLine("4 - Query");
                var option = Convert.ToInt32(System.Console.ReadLine());

                switch (option)
                {
                    case 1:
                        {
                            CreateTable();
                            break;
                        }

                    case 2:
                        {
                            SaveItem();
                            break;
                        }

                    case 3:
                        {
                            Count();
                            break;
                        }

                    case 4:
                        {
                            Query();
                            break;
                        }
                }

                System.Console.ReadKey();
            }
        }

        private static AmazonDynamoDBClient _client = new AmazonDynamoDBClient(
            ConfigurationManager.AppSettings["AWSAccessKey"], 
            ConfigurationManager.AppSettings["AWSSecretKey"], 
            RegionEndpoint.GetBySystemName(ConfigurationManager.AppSettings["AWSRegionName"]));

        private static void CreateTable()
        {
            var tableRequest = new CreateTableRequest();
            tableRequest.TableName = "NotificationPage";
            tableRequest.KeySchema = new List<KeySchemaElement>()
            {
                new KeySchemaElement() { AttributeName = "UserID", KeyType = KeyType.HASH },
                new KeySchemaElement() { AttributeName = "EventKey", KeyType = KeyType.RANGE }
            };
            tableRequest.ProvisionedThroughput = new ProvisionedThroughput()
            {
                ReadCapacityUnits = 5,
                WriteCapacityUnits = 5
            };
            tableRequest.AttributeDefinitions = new List<AttributeDefinition>()
            {
                new AttributeDefinition() { AttributeName = "UserID", AttributeType =  ScalarAttributeType.N },
                new AttributeDefinition() { AttributeName = "EventKey", AttributeType =  ScalarAttributeType.S }
            };

            var result = _client.CreateTable(tableRequest);

            System.Console.WriteLine("{0} - {1}",
                result.HttpStatusCode,
                result.TableDescription.TableStatus);
        }

        private static void SaveItem()
        {
            var userID = 2;
            var notificationEventID = 10;
            var eventPrimaryKey = 101;
            var text = "Texto da notificação";
            var eventDateTime = new DateTime(2016, 7, 8, 9, 31, 20);
            var read = false;

            var request = new PutItemRequest()
            {
                TableName = "NotificationPage",
                Item = new Dictionary<string, AttributeValue>()
                {
                    { "UserID", new AttributeValue() { N = userID.ToString() }},
                    { "EventKey", new AttributeValue() { S = string.Format("{0}_{1}_{2}", eventDateTime.ToString("yyyyMMddHHmmss"), notificationEventID.ToString(), eventPrimaryKey.ToString()) } },
                    { "NotificationEventID", new AttributeValue() { N = notificationEventID.ToString() } },
                    { "EventPrimaryKey", new AttributeValue { N = eventPrimaryKey.ToString() } },
                    { "Text", new AttributeValue { S = text } },
                    { "EventDateTime", new AttributeValue { N = eventDateTime.ToString("yyyyMMddHHmmss") } },
                    { "NotificationRead", new AttributeValue { N = read ? "1" : "0" } }
                }
            };

            _client.PutItem(request);

            System.Console.WriteLine("OK");
        }       

        private static void Count()
        {
            var request = new QueryRequest()
            {
                TableName = "NotificationPage",
                KeyConditionExpression = "UserID = :v_UserID",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> { { ":v_UserID", new AttributeValue { N = "2" } }, { ":v_Read", new AttributeValue { N = "1" } } },
                FilterExpression = "NotificationRead = :v_Read",
                Select = "COUNT",
                ConsistentRead = false
            };

            var response = _client.Query(request);

            System.Console.WriteLine("Count: {0}", response.Count);
        }

        private static void Query()
        {
            var request = new QueryRequest()
            {
                TableName = "NotificationPage",
                KeyConditionExpression = "UserID = :v_UserID",
                ExpressionAttributeValues = new Dictionary<string, AttributeValue> { { ":v_UserID", new AttributeValue { N = "2" } } },
                ConsistentRead = false
            };

            var response = _client.Query(request);

            System.Console.WriteLine("Count: {0}", response.Count);

            foreach (var item in response.Items)
            {
                foreach (var key in item.Keys)
                {
                    System.Console.Write(key);
                    AttributeValue value;
                    if (item.TryGetValue(key, out value))
                    {
                        System.Console.WriteLine(
                            (value.S == null ? "" : "S=[" + value.S + "]") +
                            (value.N == null ? "" : "N=[" + value.N + "]") +
                            (value.SS == null ? "" : "SS=[" + string.Join(",", value.SS.ToArray()) + "]") +
                            (value.NS == null ? "" : "NS=[" + string.Join(",", value.NS.ToArray()) + "]"));
                    }
                };
            }

            System.Console.WriteLine("OK");
        }
    }
}
