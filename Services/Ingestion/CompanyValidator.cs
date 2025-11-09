using System;
using System.Collections.Generic;
using System.IO;
using Npgsql;
using ProductDataIngestion.Models;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Services.Ingestion
{
    /// <summary>
    /// グループ会社コード（GP会社コード）を検証するクラス。
    /// 
    /// 【概要】
    /// - m_company テーブルから有効な会社コードを取得して照合する。
    /// - ローカル開発などで m_company テーブルが存在しない場合、
    ///   代替として定義済みの会社コードリスト（KM, RKE, KBO）を使って確認する。
    /// 
    /// 【用途】
    /// - CSV取込前に会社コードが正しいかどうかを判定し、誤った場合はエラーを投げる。
    /// - DBが利用できない環境でも最低限の検証を行えるようにする。
    /// 
    /// 【補足】
    /// - 例外が発生してもフォールバックが成功すれば“正常継続”とみなす設計。
    /// - フォールバックでも失敗すれば IngestException を上位にスロー。
    /// </summary>
    public class CompanyValidator
    {
        /// <summary>
        /// デフォルトの代替用会社コード（m_company が取得できない場合に使用）
        /// </summary>
        private static readonly HashSet<string> DefaultFallbackCodes = new(StringComparer.OrdinalIgnoreCase)
        {
            "KM", "RKE", "KBO" // ★ 基本の3社コード（KOMEHYO系）
        };

        private readonly ICompanyRepository _companyRepository;
        private readonly HashSet<string> _fallbackCodes;

        /// <summary>
        /// コンストラクタ。
        /// DBリポジトリと、オプションで代替コードリストを受け取る。
        /// </summary>
        /// <param name="companyRepository">会社マスタへのアクセスを行うリポジトリ</param>
        /// <param name="fallbackCodes">代替コードリスト（省略可）</param>
        public CompanyValidator(ICompanyRepository companyRepository, IEnumerable<string>? fallbackCodes = null)
        {
            _companyRepository = companyRepository ?? throw new ArgumentNullException(nameof(companyRepository));
            _fallbackCodes = fallbackCodes != null
                ? new HashSet<string>(fallbackCodes, StringComparer.OrdinalIgnoreCase)
                : new HashSet<string>(DefaultFallbackCodes, StringComparer.OrdinalIgnoreCase);
        }

        /// <summary>
        /// 指定されたグループ会社コードが有効であるかを検証する。
        /// 
        /// 【処理内容】
        /// 1. 文字列が空でないかチェック（空なら必須エラー）。
        /// 2. DB (m_company) から会社情報を取得。
        /// 3. 取得できなければ「該当なし」エラーを投げる。
        /// 4. 非アクティブまたは不正データならエラー。
        /// 5. DBエラー・接続エラー時は fallback リストで再検証する。
        /// </summary>
        /// <param name="groupCompanyCd">検証対象のGP会社コード</param>
        public async Task ValidateAsync(string groupCompanyCd)
        {
            // ★ Step 1: 空文字チェック
            if (string.IsNullOrWhiteSpace(groupCompanyCd))
            {
                // → CSVに会社コードが入っていないなど、入力不備
                throw new IngestException(
                    ErrorCodes.MISSING_COLUMN,
                    "GP会社コードは必須です。" // 「必須列が空」の業務エラー
                );
            }

            try
            {
                // ★ Step 2: DBから会社マスタ情報を取得
                var company = await _companyRepository.GetActiveCompanyAsync(groupCompanyCd);

                // ★ Step 3: DBに該当会社が存在しない場合
                if (company == null)
                {
                    throw new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"指定されたGP会社コードは見つかりません: {groupCompanyCd}"
                    );
                }

                // ★ Step 4: レコードは存在するが無効またはフォーマット不正
                if (!company.IsValid())
                {
                    throw new IngestException(
                        ErrorCodes.MAPPING_NOT_FOUND,
                        $"GP会社コードが無効、または形式が不正です: {groupCompanyCd}"
                    );
                }

                // ★ Step 5: 正常パス（有効会社）
                Console.WriteLine($"GP会社コードの検証完了: {company.GroupCompanyCd} - {company.GroupCompanyNm}");
            }

            // ========= 例外ハンドリング群 =========
            // 以下の catch は“エラーではあるがフォールバックで復帰可能”とみなす。

            // ★ DB構造エラー（テーブルが存在しない場合）
            catch (PostgresException ex) when (ex.SqlState == "42P01")
            {
                Console.WriteLine($"m_company テーブルが見つかりません。定義済みコードで代替検証を行います。詳細: {ex.Message}");
                ValidateWithFallback(groupCompanyCd); // → フォールバック実行
            }

            // ★ DB接続エラー（ネットワークやサーバ停止）
            catch (NpgsqlException ex)
            {
                Console.WriteLine($"m_company クエリ失敗（接続エラー）。定義済みコードで代替検証を行います。詳細: {ex.Message}");
                ValidateWithFallback(groupCompanyCd);
            }

            // ★ ファイルI/O関連の例外（ローカル環境など）
            catch (IOException ex)
            {
                Console.WriteLine($"m_company クエリ失敗（I/Oエラー）。定義済みコードで代替検証を行います。詳細: {ex.Message}");
                ValidateWithFallback(groupCompanyCd);
            }

            // ★ DB操作の無効状態（例：Connectionが閉じている）
            catch (InvalidOperationException ex)
            {
                Console.WriteLine($"m_company クエリ失敗（無効な操作）。定義済みコードで代替検証を行います。詳細: {ex.Message}");
                ValidateWithFallback(groupCompanyCd);
            }

            // ★ 業務エラー（既に上でthrowされたIngestException）を再スロー
            catch (IngestException)
            {
                // → この時点で業務的にNG（DBやfallbackでも不正）と確定している
                throw;
            }

            // ★ その他の予期しない例外（例：null参照など）
            catch (Exception ex)
            {
                Console.WriteLine($"m_company クエリ中に予期しないエラーが発生しました。定義済みコードで代替検証を行います。詳細: {ex.Message}");
                ValidateWithFallback(groupCompanyCd);
            }
        }

        /// <summary>
        /// データベースが利用できない場合に、定義済みの会社コードリストで検証を行う。
        /// 
        /// 【処理内容】
        /// - 入力が空かどうか確認。
        /// - fallback リスト内にコードが存在すればOK。
        /// - 存在しなければ MAPPING_NOT_FOUND エラーを投げる。
        /// </summary>
        /// <param name="groupCompanyCd">検証対象のGP会社コード</param>
        private void ValidateWithFallback(string groupCompanyCd)
        {
            // ★ 入力空 or 定義済みリストに含まれない場合はNG
            if (string.IsNullOrWhiteSpace(groupCompanyCd) ||
                !_fallbackCodes.Contains(groupCompanyCd.Trim()))
            {
                // → KM/RKE/KBO 以外は認識されない
                throw new IngestException(
                    ErrorCodes.MAPPING_NOT_FOUND,
                    $"GP会社コードが認識されません: {groupCompanyCd}"
                );
            }

            // ★ フォールバック成功 → そのまま継続
            Console.WriteLine($"GP会社コードが代替リストで確認されました: {groupCompanyCd}");
        }
    }
}
