// フォルダ: Utils
// ファイル名: Logger.cs

using System;
using System.IO; // Path.GetFileName を使うために必要
using System.Runtime.CompilerServices; // Caller Information Attributes を使うために必要

namespace ProductDataIngestion.Utils // あなたのプロジェクト名に合わせてください
{
    public static class Logger
    {
        // ログ出力の共通ロジックをまとめるヘルパーメソッド
        private static void Log(string level, string message, ConsoleColor? color = null,
            [CallerLineNumber] int lineNumber = 0,
            [CallerFilePath] string filePath = "",
            [CallerMemberName] string memberName = "")
        {
            // ファイルパスからファイル名だけを取得します (例: C:\...\MyFile.cs -> MyFile.cs)
            string fileName = Path.GetFileName(filePath);

            string logMessage = $"[{DateTime.Now:yyyy-MM-dd HH:mm:ss}] [{level}] [{fileName}:{memberName}:{lineNumber}] {message}";

            if (color.HasValue)
            {
                Console.ForegroundColor = color.Value;
                Console.WriteLine(logMessage);
                Console.ResetColor();
            }
            else
            {
                Console.WriteLine(logMessage);
            }
        }

        public static void Info(string message,
            [CallerLineNumber] int lineNumber = 0,
            [CallerFilePath] string filePath = "",
            [CallerMemberName] string memberName = "")
        {
            Log("INFO", message, null, lineNumber, filePath, memberName);
        }

        public static void Warn(string message,
            [CallerLineNumber] int lineNumber = 0,
            [CallerFilePath] string filePath = "",
            [CallerMemberName] string memberName = "")
        {
            Log("WARN", message, ConsoleColor.Yellow, lineNumber, filePath, memberName);
        }

        public static void Error(string message,
            [CallerLineNumber] int lineNumber = 0,
            [CallerFilePath] string filePath = "",
            [CallerMemberName] string memberName = "")
        {
            Log("ERROR", message, ConsoleColor.Red, lineNumber, filePath, memberName);
        }
        public static void Debug(string message)
        {
            #if DEBUG
            Console.WriteLine($"[DEBUG] {message}");
            #endif
        }

    }
}