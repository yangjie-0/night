using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Dapper;
using Npgsql;

namespace ProductDataIngestion.Services.Ingestion
{
    /// <summary>
    /// データベースのNOT NULL制約情報を読み込みキャッシュするユーティリティ。
    /// CSV取り込み時に必須項目のチェックへ利用する。
    /// </summary>
    public class DatabaseSchemaInspector
    {
        private readonly string _connectionString;
        private Dictionary<string, HashSet<string>>? _notNullColumnsCache;

        public DatabaseSchemaInspector(string connectionString)
        {
            _connectionString = connectionString ?? throw new ArgumentNullException(nameof(connectionString));
        }

        /// <summary>
        /// NOT NULL列のキャッシュを要求時に読み込む。
        /// </summary>
        public async Task EnsureLoadedAsync()
        {
            if (_notNullColumnsCache != null)
            {
                return;
            }

            _notNullColumnsCache = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);

            using var connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();

            const string sql = @"
                SELECT
                    table_name,
                    column_name,
                    is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name IN ('temp_product_parsed', 'temp_product_event', 'cl_product_attr')
                ORDER BY table_name, ordinal_position";

            var columns = await connection.QueryAsync<(string TableName, string ColumnName, string IsNullable)>(sql);

            foreach (var (tableName, columnName, isNullable) in columns)
            {
                if (!_notNullColumnsCache.TryGetValue(tableName, out var set))
                {
                    set = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    _notNullColumnsCache[tableName] = set;
                }

                if (isNullable == "NO")
                {
                    set.Add(columnName);
                }
            }

            Console.WriteLine("NOT NULL列のキャッシュを作成しました。");
            foreach (var (table, cols) in _notNullColumnsCache)
            {
                Console.WriteLine($"  {table}: {cols.Count}列");
            }
        }

        /// <summary>
        /// 指定テーブルの指定列がNOT NULLかどうかを返す。
        /// </summary>
        public bool IsColumnNotNull(string tableName, string columnName)
        {
            if (_notNullColumnsCache == null)
            {
                return false;
            }

            if (!_notNullColumnsCache.TryGetValue(tableName, out var columns))
            {
                return false;
            }

            return columns.Contains(columnName);
        }
    }
}
