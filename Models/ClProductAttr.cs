using System;
using System.Collections.Generic;

namespace ProductDataIngestion.Models
{
    /// <summary>
    /// クレンジング結果の商品属性を表すモデル（cl_product_attr テーブル相当）。
    /// - INGEST 段階: 主に `SourceRaw` を保持し、`Value*` 系や `DataType` は未設定（null）のことが多い。
    /// - CLEANSE 段階: 正規化された値を `ValueText`/`ValueNum`/`ValueDate` 等に設定し、`DataType` を付与する。
    /// このクラスはデータフロー上の中間表現としての用途を想定している。
    /// </summary>
    public class ClProductAttr
    {
        /// <summary>
        /// バッチ実行ID。どの処理一式で作成されたかを識別する。
        /// </summary>
        public string BatchId { get; set; } = string.Empty;

        /// <summary>
        /// 一時行の識別子（インメモリ/一時テーブル用の GUID）。
        /// </summary>
        public Guid TempRowId { get; set; }

        /// <summary>
        /// 属性コード（システム内での一意な属性識別子）。
        /// </summary>
        public string AttrCd { get; set; } = string.Empty;

        /// <summary>
        /// 属性シーケンス（同じ AttrCd 内での順序を表す）。
        /// </summary>
        public short AttrSeq { get; set; }

        /// <summary>
        /// 元データの外部ID（あれば）。元ソーステーブルのキーなど。
        /// </summary>
        public string? SourceId { get; set; }

        /// <summary>
        /// 元データで使われているラベルや見出し（あれば）。人間向けの補助情報。
        /// </summary>
        public string? SourceLabel { get; set; }

        /// <summary>
        /// 取り込んだ生データ（未加工の文字列）。INGEST 段階で主に設定される。
        /// </summary>
        public string SourceRaw { get; set; } = string.Empty;

        /// <summary>
        /// クレンジング後の文字列表現（正規化された値）。INGEST では null、CLEANSE で設定される。
        /// </summary>
        public string? ValueText { get; set; }

        /// <summary>
        /// クレンジング後の数値表現（必要な場合）。null は未設定を意味する。
        /// </summary>
        public decimal? ValueNum { get; set; }

        /// <summary>
        /// クレンジング後の日付表現（必要な場合）。
        /// </summary>
        public DateTime? ValueDate { get; set; }

        /// <summary>
        /// コード値（マスタ参照などで使う短いコード）。
        /// </summary>
        public string? ValueCd { get; set; }

        /// <summary>
        /// 候補リスト等と紐づけたリスト項目ID（あれば）。
        /// </summary>
        public long? GListItemId { get; set; }

        /// <summary>
        /// 値のデータタイプ（例: "text", "number", "date"）。INGEST では null のことが多い。
        /// </summary>
        public string? DataType { get; set; }

        /// <summary>
        /// 品質フラグ（例: "OK", "WARN", "NG"）。デフォルトは "OK"。
        /// </summary>
        public string QualityFlag { get; set; } = "OK";

        /// <summary>
        /// 品質判定の詳細を格納するJSON文字列（検証結果や理由など）。
        /// </summary>
        public string QualityDetailJson { get; set; } = "{}";

        /// <summary>
        /// 値の由来情報（どの処理で変換されたか等）を保持するJSON。
        /// </summary>
        public string ProvenanceJson { get; set; } = "{}";

        /// <summary>
        /// 適用されたルールやルールセットのバージョンを示す文字列。
        /// </summary>
        public string RuleVersion { get; set; } = string.Empty;

        /// <summary>
        /// 作成日時。DB の CURRENT_TIMESTAMP で自動設定される想定。
        /// </summary>
        public DateTime? CreAt { get; set; }

        /// <summary>
        /// 更新日時。DB の CURRENT_TIMESTAMP で自動更新される想定。
        /// </summary>
        public DateTime? UpdAt { get; set; }
    }
}