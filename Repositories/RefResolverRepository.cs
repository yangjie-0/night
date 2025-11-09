// ProductDataIngestion.Repositories/RefResolverRepository.cs
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Dapper;
using Npgsql;
using ProductDataIngestion.Models;
using ProductDataIngestion.Utils;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Repositories
{
    public class RefResolverRepository : IRefResolverRepository
    {
        private readonly string _connectionString;

        public RefResolverRepository(string connectionString)
        {
            _connectionString = connectionString;
        }

        public async Task<(string? ValueCd, string? ValueText)> ResolveAsync(
            RefTableMap refMap,
            string? sourceId,
            string? sourceLabel)
        {
            if (string.IsNullOrWhiteSpace(refMap.Hop1Table) ||
                string.IsNullOrWhiteSpace(refMap.Hop1IdCol))
            {
                Logger.Warn("警告：Hop1Table または Hop1IdCol が未設定!");
                return (null, null);
            }

            // hop1（単表参照）
            if (string.IsNullOrWhiteSpace(refMap.Hop2Table))
            {
                // hop1_table の hop1_id_col 返却値を返す
                // 例: m_company → group_company_cd を返す
                var hop1ReturnCol = NormalizeColumn(refMap.Hop1ReturnCols?.FirstOrDefault());

                var sqlSingle = $@"
                    SELECT {hop1ReturnCol} AS value_cd, NULL AS value_text
                    FROM {refMap.Hop1Table}
                    WHERE is_active = TRUE
                      AND {refMap.Hop1IdCol} = @SourceId
                    LIMIT 1;
                ";

                Logger.Info($"単表参照を実行: (table={refMap.Hop1Table}, id_col={refMap.Hop1IdCol}, return_col={hop1ReturnCol})");

                await using var conn = new NpgsqlConnection(_connectionString);
                return await conn.QueryFirstOrDefaultAsync<(string?, string?)>(
                    sqlSingle, new { SourceId = sourceId });
            }
            // hop1 + hop2（JOIN 参照）
            else
            {
                Logger.Info($"JOIN クエリを実行: (hop1={refMap.Hop1Table}, hop2={refMap.Hop2Table})");

                // refMap の定義に基づいて、JOINを含む完全なSQL文とパラメータオブジェクトを生成
                var (sqlJoin, param) = BuildJoinQueryAndParams(refMap, sourceId, sourceLabel);

                await using (var conn = new NpgsqlConnection(_connectionString))
                {
                    return await conn.QueryFirstOrDefaultAsync<(string?, string?)>(sqlJoin, param);
                }
            }
        }

        // JOIN クエリとパラメータを構築するヘルパーメソッド
        private static (string Sql, object Param) BuildJoinQueryAndParams(
            RefTableMap refMap,
            string? sourceId,
            string? sourceLabel)
        {
            // join 条件（JSON 例: {"g_brand_id":"g_brand_id"}）
            var joinInfo = ParseJoinJson(refMap.Hop2JoinOnJson);
            if (joinInfo.Count == 0)
            {
                // JOIN 定義がない場合は失敗扱いにする
                Logger.Warn($"警告: JOIN定義が存在しません (attr_cd={refMap.AttrCd}, hop1_table={refMap.Hop1Table}, hop2_table={refMap.Hop2Table})");
                return (@"SELECT NULL AS value_cd, NULL AS value_text WHERE 1=0;", new { SourceId = sourceId });
            }

            // JOINのON句に使う条件を作成
            // join条件の定義（例: {"g_brand_id":"g_brand_id"}）
            // → hop1.g_brand_id = hop2.g_brand_id という ON 条件を生成
            var joinCondition = string.Join(" AND ", joinInfo.Select(kv => $"hop1.{kv.Key} = hop2.{kv.Value}"));

            // 返却列
            var retCd = refMap.Hop2ReturnCdCol ?? "/* missing */";
            var retLbl = refMap.Hop2ReturnLabelCol ?? "/* missing */";

            // hop1_match_by の値に応じて WHERE 条件を組み立てる
            string whereLeft;

            if (refMap.Hop1MatchBy?.ToUpperInvariant() == "AUTO")
            {
                // AUTO（ID と NAME 両方マッチ）
                whereLeft = $"hop1.{refMap.Hop1IdCol} = @SourceId AND hop1.{refMap.Hop1LabelCol} = @SourceLabel";
            }
            else
            {
                // IDのみマッチ（デフォルト）
                whereLeft = $"hop1.{refMap.Hop1IdCol} = @SourceId";
            }

            var sql = $@"
                SELECT hop2.{retCd} AS value_cd,
                       hop2.{retLbl} AS value_text
                FROM {refMap.Hop1Table} AS hop1
                JOIN {refMap.Hop2Table} AS hop2
                  ON {joinCondition}
                WHERE {whereLeft}
                  AND hop1.is_active = TRUE
                  AND hop2.is_active = TRUE
                LIMIT 1;
            ";

            return (sql, new { SourceId = sourceId, SourceLabel = sourceLabel });
        }

        // hop2_join_on_json に格納されている JSON 文字列を Dictionary に変換するメソッド。
        // 例：{"g_brand_id":"g_brand_id"} → { ["g_brand_id"] = "g_brand_id" } のような形に変換する。
        private static Dictionary<string, string> ParseJoinJson(string? json)
        {
            if (string.IsNullOrWhiteSpace(json)) return new Dictionary<string, string>();
            try
            {
                var dict = JsonSerializer.Deserialize<Dictionary<string, string>>(json);
                return dict ?? new Dictionary<string, string>();
            }
            catch
            {
                return new Dictionary<string, string>();
            }
        }

        private static string NormalizeColumn(string? col)
        {
            if (string.IsNullOrWhiteSpace(col)) return "/* missing */";
            // 一部の設計で {group_company_cd} のように {} が入っているケースを想定 → 取り除く
            return col.Replace("{", "").Replace("}", "").Trim();
        }
    }
}
