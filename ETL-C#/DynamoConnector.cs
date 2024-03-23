using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.DocumentModel;
using Amazon.DynamoDBv2.Model;

using System;
using System.Collections.Generic;

public class DynamoConnector
{
    private readonly AmazonDynamoDBClient _client;

    public DynamoConnector()
    {

        string endpointUrl = "http://localhost:8000";
        _client = new AmazonDynamoDBClient(new AmazonDynamoDBConfig
        {
            ServiceURL = endpointUrl
        });
    }

    public Table GetTable(string tableName)
    {
        try
        {
            Table table = Table.LoadTable(_client, tableName);
            return table;
        }
        catch (AmazonDynamoDBException ex)
        {
            Console.WriteLine("Error getting table: " + ex.Message);
            return null;
        }
    }

    public bool CreateItem(Table table, Dictionary<string, DynamoDBEntry> itemData)
    {
        try
        {
            Document document = new Document();
            foreach (var entry in itemData)
            {
                document[entry.Key] = entry.Value;
            }
            table.PutItem(document);
            Console.WriteLine("Item created successfully!");
            return true;
        }
        catch (AmazonDynamoDBException ex)
        {
            Console.WriteLine("Error creating item: " + ex.Message);
            return false;
        }
    }

    public Document ReadItem(Table table, Dictionary<string, DynamoDBEntry> key)
    {
        try
        {
            Document document = table.GetItem(key);
            if (document != null)
            {
                Console.WriteLine("Item found: " + document.ToJson());
                return document;
            }
            else
            {
                Console.WriteLine("Item not found.");
                return null;
            }
        }
        catch (AmazonDynamoDBException ex)
        {
            Console.WriteLine("Error reading item: " + ex.Message);
            return null;
        }
    }

    public List<Document> ReadItemTable(string tableName)
    {
        try
        {
            Table table = GetTable(tableName);
            ScanFilter filter = new ScanFilter();
            Search search = table.Scan(filter);
            List<Document> items = new List<Document>();
            do
            {
                items.AddRange(search.GetNextSet());
            } while (!search.IsDone);
            return items;
        }
        catch (AmazonDynamoDBException ex)
        {
            Console.WriteLine("Error scanning table: " + ex.Message);
            return null;
        }
    }


}
