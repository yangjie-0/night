namespace ProductDataIngestion.Models
{
    /// <summary>
    /// 属性（メタデータ）定義を表すモデル（m_attr_definition）。
    /// 属性コード、表示名、データ型、選択肢情報などを保持する。
    /// </summary>
    public class MAttrDefinition
    {
        /// <summary>
        /// 属性の内部ID。
        /// </summary>
        public long AttrId { get; set; }

        /// <summary>
        /// 属性コード（ユニークキー）。
        /// </summary>
        public string AttrCd { get; set; } = string.Empty;

        /// <summary>
        /// 属性の表示名。
        /// </summary>
        public string AttrNm { get; set; } = string.Empty;

        /// <summary>
        /// 属性の表示順序。
        /// </summary>
        public short? AttrSortNo { get; set; }

        /// <summary>
        /// カテゴリコード（任意）。
        /// </summary>
        public string? GCategoryCd { get; set; }

        /// <summary>
        /// データタイプ（例: TEXT, NUM, DATE, LIST, BOOL, REF）。
        /// </summary>
        public string DataType { get; set; } = string.Empty; // TEXT, NUM, DATE, LIST, BOOL, REF

        /// <summary>
        /// 候補リストグループコード（LIST タイプの場合）。
        /// </summary>
        public string? GListGroupCd { get; set; }

        /// <summary>
        /// 選択方式（SINGLE, MULTI など）。
        /// </summary>
        public string? SelectType { get; set; } // SINGLE, MULTI

        /// <summary>
        /// Golden 属性フラグ（重要属性かどうか）。
        /// </summary>
        public bool? IsGoldenAttr { get; set; }

        /// <summary>
        /// クレンジングフェーズ（処理段階）。
        /// </summary>
        public short? CleansePhase { get; set; }

        /// <summary>
        /// 必須となるコンテキストキーの配列（例: ブランド情報が必要等）。
        /// </summary>
        public string[]? RequiredContextKeys { get; set; }

        /// <summary>
        /// ターゲットテーブル名（マッピング先）。
        /// </summary>
        public string? TargetTable { get; set; }

        /// <summary>
        /// ターゲットカラム名（マッピング先）。
        /// </summary>
        public string? TargetColumn { get; set; }

        /// <summary>
        /// 単位コード（例: cm, mm, ct, g）。
        /// </summary>
        public string? ProductUnitCd { get; set; } // cm, mm, ct, g

        /// <summary>
        /// 信用活性フラグ（用途依存）。
        /// </summary>
        public bool? CreditActiveFlag { get; set; }

        /// <summary>
        /// 使用目的（PRODUCT, CATALOG など）。
        /// </summary>
        public string? Usage { get; set; } // PRODUCT, CATALOG, NULL

        /// <summary>
        /// テーブル種別コード（MST, EAV 等）。
        /// </summary>
        public string? TableTypeCd { get; set; } // MST, EAV

        /// <summary>
        /// ゴールデン商品フラグ（商品単位）。
        /// </summary>
        public bool IsGoldenProduct { get; set; }

        /// <summary>
        /// EAV でのゴールデン属性フラグ。
        /// </summary>
        public bool IsGoldenEav { get; set; }

        /// <summary>
        /// 属性が有効かどうか。
        /// </summary>
        public bool IsActive { get; set; }

        /// <summary>
        /// 属性に関する備考。
        /// </summary>
        public string? AttrRemarks { get; set; }

        /// <summary>
        /// 作成日時。
        /// </summary>
        public DateTime CreAt { get; set; }

        /// <summary>
        /// 更新日時。
        /// </summary>
        public DateTime UpdAt { get; set; }
    }
}
