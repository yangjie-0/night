using System;
using System.Text.Json.Serialization;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// processed_columns の各列情報を表すモデルです。
    /// JSON からデシリアライズして使用します。
    /// 学習者向けに簡単な説明を付けています。
    /// </summary>
    public class ProcessedColumnInfo
    {
        [JsonPropertyName("csv_column_index")]
        /// <summary>
        /// CSV の列インデックス（0 始まり）。
        /// </summary>
        public int CsvColumnIndex { get; set; }

        [JsonPropertyName("header")]
        /// <summary>
        /// CSV の見出し（ヘッダ名）。
        /// </summary>
        public string Header { get; set; } = string.Empty;

        [JsonPropertyName("raw_value")]
        /// <summary>
        /// 元の生データ（未加工）。
        /// </summary>
        public string RawValue { get; set; } = string.Empty;

        [JsonPropertyName("transformed_value")]
        /// <summary>
        /// 変換・クレンジング後の値。
        /// 属性の source_id/label に使われることがあります。
        /// </summary>
        public string TransformedValue { get; set; } = string.Empty;

        [JsonPropertyName("target_column")]
        /// <summary>
        /// マッピング先の列名（例: brand_id）。
        /// </summary>
        public string TargetColumn { get; set; } = string.Empty;

        [JsonPropertyName("projection_kind")]
        /// <summary>
        /// 投影の種類（例: PRODUCT, PRODUCT_EAV）。
        /// </summary>
        public string ProjectionKind { get; set; } = string.Empty;

        [JsonPropertyName("attr_cd")]
        /// <summary>
        /// 属性コード（m_attr_definition で使うコード）。
        /// </summary>
        public string AttrCd { get; set; } = string.Empty;

        [JsonPropertyName("transform_expr")]
        /// <summary>
        /// 変換式（現時点では未使用）。
        /// </summary>
        public string TransformExpr { get; set; } = string.Empty;

        [JsonPropertyName("is_required")]
        /// <summary>
        /// この列が必須かどうか（true = 必須）。
        /// </summary>
        public bool IsRequired { get; set; }

        [JsonPropertyName("is_injected")]
        /// <summary>
        /// 自動で挿入された列か（現時点では未使用）。
        /// </summary>
        public bool IsInjected { get; set; }

        [JsonPropertyName("mapping_success")]
        /// <summary>
        /// マッピングが成功したか（true/false/null）。
        /// </summary>
        public bool? MappingSuccess { get; set; }
    }
}
