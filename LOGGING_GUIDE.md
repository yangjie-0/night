# ログ出力詳細説明

## 📋 概要

本ドキュメントは、ProductDataIngestion システムにおけるログ出力の詳細を説明します。
各サービスクラスでのログ出力箇所、目的、重要度を網羅しています。

---

## 🏗️ ログ出力を行うクラス

### 1. IngestService.cs

**役割**: CSV取込処理のメインフロー統括

#### 📍 ログ出力箇所一覧

##### フロー開始・終了

```csharp
// 行53: 処理開始ログ
Console.WriteLine($"=== CSV取込開始 ===\nファイル: {filePath}\nGP会社: {groupCompanyCd}\n処理モード: {targetEntity}");

// 行79: 処理完了ログ
Console.WriteLine($"=== 取込完了 ===\n読込: {result.readCount}\n成功: {result.okCount}\n失敗: {result.ngCount}");
```

**目的**: 処理全体の開始・終了を明確に記録
**重要度**: ★★★ (必須)

---

##### フロー1: バッチ起票

```csharp
// 行133: バッチ起票成功
Console.WriteLine($"バッチ起票完了: {batchId}");
```

**目的**: バッチIDの記録、後続処理での参照用
**重要度**: ★★★ (必須)

---

##### フロー2: ルール取得

```csharp
// 行176: ルール取得成功
Console.WriteLine($"取込ルール取得完了: ProfileId={importSetting.ProfileId}, 列数={importDetails.Count}");
```

**目的**: 取込設定の確認、列マッピング数の確認
**重要度**: ★★☆ (推奨)

---

##### フロー4: ヘッダー読込

```csharp
// 行281: ヘッダー取得成功
Console.WriteLine($"ヘッダー取得完了: {headers.Length} 列");
```

**目的**: CSV列数の確認
**重要度**: ★★☆ (推奨)

---

##### フロー6: temp保存

```csharp
// 行638: temp保存完了
Console.WriteLine($"temp保存完了: 商品={_tempProducts.Count}, エラー={_recordErrors.Count}");
```

**目的**: 保存件数の確認、エラー件数の確認
**重要度**: ★★★ (必須)

---

##### フロー10: バッチ統計更新

```csharp
// 行540: バッチ統計更新完了
Console.WriteLine($"バッチ統計更新完了: {batchRun.BatchStatus}");

// 行615: バッチ失敗マーク
Console.WriteLine($"バッチ失敗: {errorMessage}");

// 行620: バッチ失敗マーク中のエラー
Console.WriteLine($"バッチ失敗マーク中にエラー: {ex.Message}");
```

**目的**: 最終ステータスの記録、エラー時のフェイルセーフ
**重要度**: ★★★ (必須)

---

##### エラー記録

```csharp
// 行575: エラーレコード記録
Console.WriteLine($"エラーレコード: [{error.ErrorCd}] {error.ErrorDetail}");
```

**目的**: 個別レコードエラーの詳細記録
**重要度**: ★★★ (必須)

---

##### GP会社検証

```csharp
// 行902: GP会社検証成功
Console.WriteLine($"GP会社検証成功: {company.GroupCompanyCd} - {company.GroupCompanyNm}");

// 行933: GP会社コード簡易検証
Console.WriteLine($"GP会社コード簡易検証: {groupCompanyCd}");
```

**目的**: 会社コードの妥当性確認
**重要度**: ★★☆ (推奨)

---

##### Transform Expression デバッグログ (警告・エラー)

```csharp
// 行740: フォーマット解析失敗警告
Console.WriteLine($"[警告] 日付フォーマット解析失敗: {expression}");

// 行765: 日付パース失敗警告
Console.WriteLine($"[警告] 日付パース失敗: value='{value}', format='{dotNetFormat}'");

// 行772: 日付変換エラー
Console.WriteLine($"[エラー] 日付変換エラー: {ex.Message}");
```

**目的**: transform_expr のデバッグ、データ品質確認
**重要度**: ★☆☆ (オプション、本番では削除可能)

---

### 2. AttributeProcessor.cs

**役割**: extras_json 解析と属性生成専門

#### 📍 ログ出力箇所一覧

##### extras_json 解析開始

```csharp
// 行101: 解析開始
Console.WriteLine($"\n=== extras_json解析開始 (temp_row_id={tempProduct.TempRowId}) ===");

// 行105: 抽出結果
Console.WriteLine($"✓ extras_jsonから抽出: processed_columns={processedColumns.Count}件, source_raw={sourceRaw.Count}件");
```

**目的**: JSON解析の開始と結果件数の確認
**重要度**: ★★☆ (推奨)

---

##### processed_columns 内容出力 (デバッグ用)

```csharp
// 行108-117: 先頭5件の詳細出力
foreach (var kvp in processedColumns.Take(5))
{
    Console.WriteLine($"  [{kvp.Key}] header={kvp.Value.Header}, attr_cd={kvp.Value.AttrCd}, " +
                    $"projection_kind={kvp.Value.ProjectionKind}, is_required={kvp.Value.IsRequired}, " +
                    $"transformed_value={kvp.Value.TransformedValue}");
}
if (processedColumns.Count > 5)
{
    Console.WriteLine($"  ... 他 {processedColumns.Count - 5} 件");
}
```

**目的**: データ内容の確認、デバッグ支援
**重要度**: ★☆☆ (デバッグ用、本番では削除可能)

---

##### フィルタ後の処理対象列数

```csharp
// 行131: フィルタ結果
Console.WriteLine($"\n✓ フィルタ後の処理対象列数: {requiredColumns.Count} (PRODUCT + PRODUCT_EAV)");
```

**目的**: is_required=true の列数確認
**重要度**: ★★☆ (推奨)

---

##### 各列の処理状況 (詳細ログ)

```csharp
// 行137-140: 列処理開始
Console.WriteLine($"\n--- 処理中: {columnKvp.Key} ---");
Console.WriteLine($"  attr_cd={columnInfo.AttrCd}, header={columnInfo.Header}");
Console.WriteLine($"  transformed_value='{columnInfo.TransformedValue}'");
Console.WriteLine($"  projection_kind={columnInfo.ProjectionKind}, is_required={columnInfo.IsRequired}");

// 行152: 変換後の値が空でスキップ
Console.WriteLine($"  → [スキップ] 変換後の値が空");

// 行163: m_fixed_to_attr_map に存在
Console.WriteLine($"  → ケース1: m_fixed_to_attr_mapに存在 (value_role={attrMap.ValueRole})");

// 行177: m_fixed_to_attr_map に存在しない
Console.WriteLine($"  → ケース2: m_fixed_to_attr_mapに存在しない");

// 行191: 属性追加成功
Console.WriteLine($"  ✓ 属性追加成功: source_id={productAttr.SourceId}, source_label={productAttr.SourceLabel}");

// 行195: 属性がnull
Console.WriteLine($"  → [スキップ] 属性がnull");
```

**目的**: 各列の詳細な処理状況の追跡、デバッグ支援
**重要度**: ★☆☆ (デバッグ用、本番では大幅削減可能)

---

##### 属性処理完了

```csharp
// 行199: 処理完了
Console.WriteLine($"属性処理完了: {productAttrs.Count}件");
```

**目的**: 生成された属性レコード数の確認
**重要度**: ★★★ (必須)

---

##### ProcessWithFixedMap 内のログ

```csharp
// 行252: 値が空でスキップ
Console.WriteLine($"[スキップ] attr_cd={columnInfo.AttrCd}: 値が空");

// 行285: 属性追加 (FixedMap使用)
Console.WriteLine($"[FixedMap] 属性追加: attr_cd={columnInfo.AttrCd}, value_role={attrMap.ValueRole}, source_id={sourceIdValue}, source_label={sourceLabelValue}");
```

**目的**: m_fixed_to_attr_map を使用した処理の詳細追跡
**重要度**: ★☆☆ (デバッグ用)

---

##### ProcessWithoutFixedMap 内のログ

```csharp
// 行305: 値が空でスキップ
Console.WriteLine($"[スキップ] attr_cd={columnInfo.AttrCd}: 値が空");

// 行335: 属性追加 (FixedMapなし)
Console.WriteLine($"[NoFixedMap] 属性追加: attr_cd={columnInfo.AttrCd}, source_id={value}");
```

**目的**: m_fixed_to_attr_map を使用しない処理の詳細追跡
**重要度**: ★☆☆ (デバッグ用)

---

##### FindValueBySourceColumn 内のログ

```csharp
// 行348: sourceColumnが空
Console.WriteLine($"    [FindValue] sourceColumnが空");

// 行355: 検索対象の確認
Console.WriteLine($"    [FindValue] sourceColumn={sourceColumn} → targetColumn={targetColumn}");

// 行361: 見つかった
Console.WriteLine($"    [FindValue] ✓ 見つかった: target_column={kvp.TargetColumn}, transformed_value='{kvp.TransformedValue}'");

// 行366: 見つからない
Console.WriteLine($"    [FindValue] × 見つからない: targetColumn={targetColumn}");
```

**目的**: source_column からの値検索プロセスの詳細追跡
**重要度**: ★☆☆ (デバッグ用、本番では削除推奨)

---

### 3. DataImportService.cs

**役割**: マスタデータ取得専門

#### 📍 ログ出力箇所一覧

```csharp
// 行179: Import setting not found
_logger?.LogError(msg);
```

**目的**: 取込設定が見つからない場合のエラーログ
**重要度**: ★★★ (必須)
**備考**: ILogger を使用 (オプション依存注入)

---

### 4. CsvValidator.cs

**役割**: CSV検証専門

#### 📍 ログ出力箇所一覧

```csharp
// 行44: 列マッピング検証完了
Console.WriteLine($"列マッピング検証完了: CSV列数={headers.Length}, 必須列エラー={requiredCount}");
```

**目的**: CSV列数と必須列エラー数の確認
**重要度**: ★★☆ (推奨)

---

## 📊 ログレベル分類

### ★★★ 必須ログ (本番環境で必須)

1. **処理開始・終了**: 処理全体のライフサイクル追跡
2. **バッチID**: トレーサビリティ確保
3. **件数情報**: 読込・成功・失敗の件数
4. **エラー記録**: 個別エラーの詳細
5. **最終ステータス**: SUCCESS/PARTIAL/FAILED

### ★★☆ 推奨ログ (本番環境で推奨)

1. **ルール取得結果**: 設定の妥当性確認
2. **CSV列数**: データ構造の確認
3. **フィルタ結果**: 処理対象件数の確認
4. **GP会社検証**: セキュリティ・データ整合性確認

### ★☆☆ デバッグログ (開発・検証環境のみ)

1. **processed_columns 詳細**: JSON内容の確認
2. **各列の処理状況**: 詳細な処理フロー追跡
3. **Transform Expression 警告**: データ品質の詳細確認
4. **FindValue プロセス**: マッピングロジックの追跡

---

## 🎯 本番環境でのログ最適化推奨

### 削除推奨ログ

```csharp
// AttributeProcessor.cs
// 行108-117: processed_columns の詳細出力 (先頭5件) → 削除
// 行137-195: 各列の処理状況の詳細ログ → 簡略化 or 削除
// 行348-366: FindValueBySourceColumn の詳細ログ → 削除

// IngestService.cs
// 行740, 765, 772: Transform Expression の警告ログ → 削減
```

### 簡略化推奨ログ

```csharp
// Before (詳細)
Console.WriteLine($"\n--- 処理中: {columnKvp.Key} ---");
Console.WriteLine($"  attr_cd={columnInfo.AttrCd}, header={columnInfo.Header}");
Console.WriteLine($"  transformed_value='{columnInfo.TransformedValue}'");
Console.WriteLine($"  projection_kind={columnInfo.ProjectionKind}, is_required={columnInfo.IsRequired}");

// After (簡略)
// ログなし (エラー時のみ出力)
```

---

## 🔧 ログ設定の推奨構造

### 環境別ログレベル

```json
// appsettings.Development.json
{
  "Logging": {
    "LogLevel": {
      "Default": "Debug",
      "ProductDataIngestion": "Debug"
    }
  }
}

// appsettings.Production.json
{
  "Logging": {
    "LogLevel": {
      "Default": "Information",
      "ProductDataIngestion": "Warning"
    }
  }
}
```

### ILogger 導入推奨

現在は `Console.WriteLine` を使用していますが、将来的には `ILogger<T>` への移行を推奨:

```csharp
// 現在
Console.WriteLine($"バッチ起票完了: {batchId}");

// 推奨 (将来)
_logger.LogInformation("バッチ起票完了: {BatchId}", batchId);
```

**メリット**:
- ログレベルの制御が容易
- 構造化ログ (JSON) 対応
- 外部ログシステム (Serilog, NLog) への統合が容易

---

## 📝 まとめ

### 現在のログ出力状況

- **IngestService**: 19箇所のログ出力 (必須8, 推奨5, デバッグ6)
- **AttributeProcessor**: 17箇所のログ出力 (必須2, 推奨2, デバッグ13)
- **CsvValidator**: 1箇所のログ出力 (推奨1)
- **DataImportService**: 1箇所のログ出力 (必須1, ILogger使用)

### 本番環境での推奨

1. ✅ **必須ログ (11箇所)** は保持
2. ✅ **推奨ログ (8箇所)** は保持
3. ⚠️ **デバッグログ (19箇所)** は削減または条件付き出力

### 将来の改善方向

1. `Console.WriteLine` → `ILogger<T>` への移行
2. 環境別ログレベルの設定
3. 構造化ログの導入
4. 外部ログ収集システムとの統合

---

**作成日**: 2025-10-22
**バージョン**: 1.0
**対象システム**: ProductDataIngestion v2.0
