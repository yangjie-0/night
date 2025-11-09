// 文件夹: Models
// 文件名: AttributeDefinition.cs

using System;
using System.Text.Json.Serialization;
using System.ComponentModel.DataAnnotations.Schema;

#nullable enable

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 映射 m_attr_definition (項目定義マスタ) 表。
    /// 这个类定义了系统中每个数据属性的静态特征、处理规则和最终存储目标。
    /// </summary>
    public class AttributeDefinition
    {
        // 項目ID
        [Column("attr_id")]
        [JsonPropertyName("attrId")]
        public long AttrId { get; set; }

        // 項目コード
        [Column("attr_cd")]
        [JsonPropertyName("attrCd")]
        public string AttrCd { get; set; } = string.Empty;

        // 項目名称
        [Column("attr_nm")]
        [JsonPropertyName("attrNm")]
        public string AttrNm { get; set; } = string.Empty;

        // 表示順
        [Column("attr_sort_no")]
        [JsonPropertyName("attrSortNo")]
        public short? AttrSortNo { get; set; }

        // Gカテゴリコード
        [Column("g_category_cd")]
        [JsonPropertyName("gCategoryCd")]
        public string? GCategoryCd { get; set; }

        // データタイプ
        [Column("data_type")]
        [JsonPropertyName("dataType")]
        public string DataType { get; set; } = string.Empty;

        // リストグループコード
        [Column("g_list_group_cd")]
        [JsonPropertyName("gListGroupCd")]
        public string? GListGroupCd { get; set; }

        // セレクトタイプ
        [Column("select_type")]
        [JsonPropertyName("selectType")]
        public string? SelectType { get; set; }

        // 正規化厳密対象
        [Column("is_golden_attr")]
        [JsonPropertyName("isGoldenAttr")]
        public bool? IsGoldenAttr { get; set; }

       // 優先度
        [Column("cleanse_phase")]
        [JsonPropertyName("cleansePhase")]
        public short? CleansePhase { get; set; } = 1;

        // 処理設定
        [Column("required_context_keys")]
        [JsonPropertyName("requiredContextKeys")]
        public string[]? RequiredContextKeys { get; set; }

        // 保存対象テーブル
        [Column("target_table")]
        [JsonPropertyName("targetTable")]
        public string? TargetTable { get; set; }

        // 保存対象カラム
        [Column("target_column")]
        [JsonPropertyName("targetColumn")]
        public string? TargetColumn { get; set; }

        // 単位コード
        [Column("product_unit_cd")]
        [JsonPropertyName("productUnitCd")]
        public string? ProductUnitCd { get; set; }

        // 単位適用フラグ
        [Column("credit_active_flag")]
        [JsonPropertyName("creditActiveFlag")]
        public bool? CreditActiveFlag { get; set; } = false;

        // 用途
        [Column("usage")]
        [JsonPropertyName("usage")]
        public string? Usage { get; set; }

        // テーブル種別コード
        [Column("table_type_cd")]
        [JsonPropertyName("tableTypeCd")]
        public string? TableTypeCd { get; set; }

        // G商品レコード昇格フラグ
        [Column("is_golden_product")]
        [JsonPropertyName("isGoldenProduct")]
        public bool IsGoldenProduct { get; set; }

        // G商品レコードEAV昇格フラグ
        [Column("is_golden_eav")]
        [JsonPropertyName("isGoldenAttrEav")]
        public bool IsGoldenAttrEav { get; set; }

        // 有効フラグ
        [Column("is_active")]
        [JsonPropertyName("isActive")]
        public bool IsActive { get; set; } = true;

        // 備考
        [Column("attr_remarks")]
        [JsonPropertyName("attrRemarks")]
        public string? AttrRemarks { get; set; }

        // 登録日時
        [Column("cre_at")]
        [JsonPropertyName("createdAt")]
        public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

        // 更新日時
        [Column("upd_at")]
        [JsonPropertyName("updatedAt")]
        public DateTime UpdatedAt { get; set; } = DateTime.UtcNow;
    }
}