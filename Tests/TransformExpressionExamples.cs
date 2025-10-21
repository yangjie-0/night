using System;
using System.Globalization;

namespace ProductDataIngestion.Tests
{
    /// <summary>
    /// Transform Expression の動作確認用サンプルコード
    ///
    /// 実行方法:
    /// このクラスは参考用のサンプルコードです。
    /// 実際のテストは IngestService クラスの ApplyTransformExpression メソッドを通じて行われます。
    /// </summary>
    public class TransformExpressionExamples
    {
        /// <summary>
        /// すべてのサンプルを実行
        /// </summary>
        public static void RunAllExamples()
        {
            Console.WriteLine("=== Transform Expression サンプル実行 ===\n");

            Example1_Trim();
            Example2_Upper();
            Example3_TrimAndUpper();
            Example4_NullIf();
            Example5_ToTimestamp_YYYYMMDD();
            Example6_ToTimestamp_DDMMYYYY();
            Example7_TrimAndNullIf();
            Example8_DateParseFailure();

            Console.WriteLine("\n=== すべてのサンプル実行完了 ===");
        }

        /// <summary>
        /// 例1: trim(@) - 前後のスペース削除
        /// </summary>
        static void Example1_Trim()
        {
            Console.WriteLine("【例1】 trim(@) - 前後のスペース削除");

            var testCases = new[]
            {
                ("  Hello  ", "Hello"),
                ("　こんにちは　", "こんにちは"),  // 全角スペース
                ("  Mixed　Space  ", "Mixed　Space")  // 内部スペースは保持
            };

            foreach (var (input, expected) in testCases)
            {
                var result = ApplyTrim(input);
                var status = result == expected ? "✓" : "✗";
                Console.WriteLine($"  {status} 入力: \"{input}\" → 出力: \"{result}\" (期待: \"{expected}\")");
            }
            Console.WriteLine();
        }

        /// <summary>
        /// 例2: upper(@) - 大文字変換
        /// </summary>
        static void Example2_Upper()
        {
            Console.WriteLine("【例2】 upper(@) - 大文字変換");

            var testCases = new[]
            {
                ("hello", "HELLO"),
                ("Hello World", "HELLO WORLD"),
                ("test123", "TEST123")
            };

            foreach (var (input, expected) in testCases)
            {
                var result = input.ToUpper();
                var status = result == expected ? "✓" : "✗";
                Console.WriteLine($"  {status} 入力: \"{input}\" → 出力: \"{result}\" (期待: \"{expected}\")");
            }
            Console.WriteLine();
        }

        /// <summary>
        /// 例3: trim(@) + upper(@) - 組み合わせ
        /// </summary>
        static void Example3_TrimAndUpper()
        {
            Console.WriteLine("【例3】 trim(@) + upper(@) - 組み合わせ");

            var testCases = new[]
            {
                ("  hello  ", "HELLO"),
                ("  Product Code  ", "PRODUCT CODE")
            };

            foreach (var (input, expected) in testCases)
            {
                var result = ApplyTrim(input).ToUpper();
                var status = result == expected ? "✓" : "✗";
                Console.WriteLine($"  {status} 入力: \"{input}\" → 出力: \"{result}\" (期待: \"{expected}\")");
            }
            Console.WriteLine();
        }

        /// <summary>
        /// 例4: nullif(@,'') - 空文字をnullに変換
        /// </summary>
        static void Example4_NullIf()
        {
            Console.WriteLine("【例4】 nullif(@,'') - 空文字をnullに変換");

            var testCases = new[]
            {
                ("", null),
                ("   ", null),  // 空白のみ
                ("value", "value")
            };

            foreach (var (input, expected) in testCases)
            {
                var result = ApplyNullIf(input);
                var status = result == expected ? "✓" : "✗";
                var displayResult = result ?? "null";
                var displayExpected = expected ?? "null";
                Console.WriteLine($"  {status} 入力: \"{input}\" → 出力: {displayResult} (期待: {displayExpected})");
            }
            Console.WriteLine();
        }

        /// <summary>
        /// 例5: to_timestamp(@,'YYYY-MM-DD') - 日付変換
        /// </summary>
        static void Example5_ToTimestamp_YYYYMMDD()
        {
            Console.WriteLine("【例5】 to_timestamp(@,'YYYY-MM-DD') - 日付変換");

            var testCases = new[]
            {
                ("2025-10-22", "2025-10-22"),
                ("2025-01-01", "2025-01-01")
            };

            foreach (var (input, expected) in testCases)
            {
                var result = ParseDate(input, "yyyy-MM-dd");
                var status = result == expected ? "✓" : "✗";
                Console.WriteLine($"  {status} 入力: \"{input}\" → 出力: \"{result}\" (期待: \"{expected}\")");
            }
            Console.WriteLine();
        }

        /// <summary>
        /// 例6: to_timestamp(@,'DD/MM/YYYY') - 異なる日付フォーマット
        /// </summary>
        static void Example6_ToTimestamp_DDMMYYYY()
        {
            Console.WriteLine("【例6】 to_timestamp(@,'DD/MM/YYYY') - 異なる日付フォーマット");

            var testCases = new[]
            {
                ("22/10/2025", "2025-10-22"),
                ("01/01/2025", "2025-01-01")
            };

            foreach (var (input, expected) in testCases)
            {
                var result = ParseDate(input, "dd/MM/yyyy");
                var status = result == expected ? "✓" : "✗";
                Console.WriteLine($"  {status} 入力: \"{input}\" → 出力: \"{result}\" (期待: \"{expected}\")");
            }
            Console.WriteLine();
        }

        /// <summary>
        /// 例7: trim + nullif - 組み合わせ (空白をnullに)
        /// </summary>
        static void Example7_TrimAndNullIf()
        {
            Console.WriteLine("【例7】 trim(@) + nullif(@,'') - 空白をnullに");

            var testCases = new[]
            {
                ("   ", null),
                ("  value  ", "value")
            };

            foreach (var (input, expected) in testCases)
            {
                var trimmed = ApplyTrim(input);
                var result = ApplyNullIf(trimmed);
                var status = result == expected ? "✓" : "✗";
                var displayResult = result ?? "null";
                var displayExpected = expected ?? "null";
                Console.WriteLine($"  {status} 入力: \"{input}\" → 出力: {displayResult} (期待: {displayExpected})");
            }
            Console.WriteLine();
        }

        /// <summary>
        /// 例8: 日付パース失敗 - 元の値を返す
        /// </summary>
        static void Example8_DateParseFailure()
        {
            Console.WriteLine("【例8】 日付パース失敗 - 元の値を返す");

            var testCases = new[]
            {
                ("invalid-date", "invalid-date"),
                ("2025/13/99", "2025/13/99")  // 無効な日付
            };

            foreach (var (input, expected) in testCases)
            {
                var result = ParseDateSafe(input, "yyyy-MM-dd");
                var status = result == expected ? "✓" : "✗";
                Console.WriteLine($"  {status} 入力: \"{input}\" → 出力: \"{result}\" (期待: \"{expected}\")");
            }
            Console.WriteLine();
        }

        #region ヘルパーメソッド

        /// <summary>
        /// trim 処理の実装
        /// </summary>
        static string ApplyTrim(string input)
        {
            return input.Trim().Trim('\u3000');  // 半角・全角スペース削除
        }

        /// <summary>
        /// nullif 処理の実装
        /// </summary>
        static string? ApplyNullIf(string input)
        {
            return string.IsNullOrWhiteSpace(input) ? null : input;
        }

        /// <summary>
        /// 日付パース (成功時のみ)
        /// </summary>
        static string ParseDate(string input, string format)
        {
            if (DateOnly.TryParseExact(input.Trim(), format, CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
            {
                return date.ToString("yyyy-MM-dd");
            }
            throw new FormatException($"日付パース失敗: {input}");
        }

        /// <summary>
        /// 日付パース (安全版: 失敗時は元の値を返す)
        /// </summary>
        static string ParseDateSafe(string input, string format)
        {
            if (DateOnly.TryParseExact(input.Trim(), format, CultureInfo.InvariantCulture, DateTimeStyles.None, out var date))
            {
                return date.ToString("yyyy-MM-dd");
            }
            return input;  // パース失敗時は元の値を返す
        }

        #endregion
    }
}
