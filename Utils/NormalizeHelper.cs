using System;
using System.Globalization;
using System.Text;
using ProductDataIngestion.Utils;

namespace ProductDataIngestion.Utils
{
    /// <summary>
    /// TEXT / NUM / TIMESTAMPTZ 型の正規化（Normalize）処理を行う共通ユーティリティクラス。
    /// </summary>
    public static class NormalizeHelper
    {
        /// <summary>
        /// 文字列型（TEXT）の正規化。
        /// Trim
        /// </summary>
        public static string NormalizeText(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
            {
                Logger.Warn("NormalizeText: 空文字を検出");
                return string.Empty;
            }

            string normalized = input.Trim();
            Logger.Warn($"NormalizeText: '{input}' → '{normalized}'");
            return normalized;
        }

        /// <summary>
        /// 数値型（NUM）の正規化。
        /// 文字・カンマ・円記号・％を除去し数値化、小数点統一。
        /// </summary>
        public static decimal NormalizeNumber(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                throw new Exception("empty_input");

            // 前後の空白を除去
            string cleaned = input.Trim();

            // カンマ・円記号・％などを削除
            cleaned = cleaned
                .Replace("¥", "")
                .Replace(",", "")
                .Replace("%", "")
                .Replace("．", "."); // 小数点を統一

            // 数値に変換
            if (decimal.TryParse(cleaned, NumberStyles.Any, CultureInfo.InvariantCulture, out var result))
            {
                Logger.Info($"NormalizeNumber: '{input}' → {result}");
                return result;
            }

            Logger.Error($"NormalizeNumber: 数値変換に失敗 (input='{input}')");
            throw new Exception("invalid_number_format");
        }

        /// <summary>
        /// 日付型（TIMESTAMPTZ）の正規化。
        /// 文字列から日付フォーマットを判定して DateTime に変換。
        /// </summary>
        public static DateTime NormalizeDate(string input)
        {
            if (string.IsNullOrWhiteSpace(input))
                throw new Exception("empty_date_input");

            string[] formats = {
            "yyyy/MM/dd",
            "yyyy-MM-dd",
            "yyyyMMdd",
            "MM/dd/yyyy",
            "yyyy/MM/dd HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy/MM/dd HH:mm",
            "yyyy-MM-dd HH:mm"
        };

            if (DateTime.TryParseExact(
                    input.Trim(),
                    formats,
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.None,
                    out var result))
            {
                Logger.Info($"NormalizeDate: '{input}' → {result:yyyy-MM-dd HH:mm:ss}");
                return result;
            }

            // DateTime.TryParse で最終的に再挑戦（柔軟パース）
            if (DateTime.TryParse(input, out var parsed))
            {
                Logger.Info($"NormalizeDate (fallback): '{input}' → {parsed:yyyy-MM-dd HH:mm:ss}");
                return parsed;
            }

            Logger.Warn($"NormalizeDate: 日付変換に失敗 (input='{input}')");
            throw new Exception("invalid_date_format");
        }
    }
}
