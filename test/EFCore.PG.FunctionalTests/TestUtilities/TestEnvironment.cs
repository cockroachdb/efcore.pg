﻿using System;
using System.IO;
using Microsoft.Extensions.Configuration;

namespace Npgsql.EntityFrameworkCore.PostgreSQL.TestUtilities
{
    public static class TestEnvironment
    {
        public static IConfiguration Config { get; }

        static TestEnvironment()
        {
            var configBuilder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("config.json", optional: true)
                .AddJsonFile("config.test.json", optional: true)
                .AddEnvironmentVariables();

            Config = configBuilder.Build()
                .GetSection("Test:Npgsql");

            System.Threading.Thread.CurrentThread.CurrentCulture = new System.Globalization.CultureInfo("en-US");
        }

        const string DefaultConnectionString = "Server=localhost;Port=26257Username=root;";

        public static string DefaultConnection => Config["DefaultConnection"] ?? DefaultConnectionString;

        static Version _postgresVersion;

        public static Version PostgresVersion
        {
            get
            {
                if (_postgresVersion != null)
                    return _postgresVersion;
                using var conn = new NpgsqlConnection(NpgsqlTestStore.CreateConnectionString("defaultdb"));
                conn.Open();
                return _postgresVersion = conn.PostgreSqlVersion;
            }
        }
    }
}
