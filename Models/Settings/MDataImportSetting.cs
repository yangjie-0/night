/// <summary>
/// データ取り込みプロファイルの設定モデル。
/// ファイルの文字コード、区切り文字、ヘッダ行位置など取り込みに必要な設定を保持する。
/// </summary>
public class MDataImportSetting
{
    /// <summary>
    /// プロファイルID。
    /// </summary>
    public long ProfileId { get; set; }

    /// <summary>
    /// 利用用途名。
    /// </summary>
    public string UsageNm { get; set; } = string.Empty;

    /// <summary>
    /// グループ会社コード。
    /// </summary>
    public string GroupCompanyCd { get; set; } = string.Empty;

    /// <summary>
    /// 対象エンティティ名。
    /// </summary>
    public string TargetEntity { get; set; } = string.Empty;

    /// <summary>
    /// 文字コード（省略可）。
    /// </summary>
    public string? CharacterCd { get; set; }

    /// <summary>
    /// 区切り文字（省略可）。
    /// </summary>
    public string? Delimiter { get; set; }

    /// <summary>
    /// ヘッダ行のインデックス。
    /// </summary>
    public int HeaderRowIndex { get; set; }

    /// <summary>
    /// スキップする先頭行数（デフォルト 0）。
    /// </summary>
    

    /// <summary>
    /// プロファイルが有効かどうか。
    /// </summary>
    public bool IsActive { get; set; }

    /// <summary>
    /// 設定に関する備考。
    /// </summary>
    public string? ImportSettingRemarks { get; set; }
}
