# ProductDataIngestion 项目架构文档

## 📋 目录

1. [项目概述](#项目概述)
2. [架构设计](#架构设计)
3. [入口层 (Program.cs)](#入口层-programcs)
4. [数据模型层 (Models/)](#数据模型层-models)
5. [数据访问层 (Repositories/)](#数据访问层-repositories)
6. [业务逻辑层 (Services/)](#业务逻辑层-services)
7. [数据流程](#数据流程)
8. [错误处理机制](#错误处理机制)

---

## 项目概述

**ProductDataIngestion** 是一个商品数据导入系统,负责从CSV文件读取商品数据,经过验证、转换后存储到PostgreSQL数据库中。系统采用标准的**三层架构设计**,确保代码的可维护性、可扩展性和可测试性。

### 核心功能
- CSV文件解析与验证
- 灵活的列映射与数据转换
- EAV (Entity-Attribute-Value) 模型支持
- 批次跟踪与错误记录
- 事务性数据处理

### 技术栈
- **.NET 8.0** - 应用框架
- **PostgreSQL** - 数据存储
- **Dapper** - 数据访问ORM
- **CsvHelper** - CSV解析
- **Npgsql** - PostgreSQL连接器

---

## 架构设计

项目采用经典的**三层架构 (3-Tier Architecture)**:

```
┌─────────────────────────────────────────┐
│         入口层 (Presentation)           │
│            Program.cs                   │
│  ┌─────────────────────────────────┐   │
│  │ • 应用程序入口                   │   │
│  │ • 配置文件读取                   │   │
│  │ • 依赖注入 & 服务组装            │   │
│  └─────────────────────────────────┘   │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────┴───────────────────────┐
│    业务逻辑层 (Business Logic)          │
│           Services/                     │
│  ┌─────────────────────────────────┐   │
│  │ • 业务规则实现                   │   │
│  │ • CSV处理流程编排                │   │
│  │ • 数据验证与转换                 │   │
│  │ • 异常处理                       │   │
│  └─────────────────────────────────┘   │
└─────────────────┬───────────────────────┘
                  │
┌─────────────────┴───────────────────────┐
│     数据访问层 (Data Access)            │
│      Models/ + Repositories/            │
│  ┌─────────────────────────────────┐   │
│  │ Models:                          │   │
│  │ • 数据模型定义                   │   │
│  │ • 错误代码常量                   │   │
│  │                                  │   │
│  │ Repositories:                    │   │
│  │ • 数据库操作封装                 │   │
│  │ • SQL查询执行                    │   │
│  └─────────────────────────────────┘   │
└─────────────────┬───────────────────────┘
                  │
         ┌────────┴────────┐
         │  PostgreSQL DB  │
         └─────────────────┘
```

### 架构优势

| 优势 | 说明 |
|-----|------|
| **关注点分离** | 每层职责清晰,相互独立 |
| **可维护性** | 修改一层不影响其他层 |
| **可测试性** | 每层可独立进行单元测试 |
| **可扩展性** | 易于添加新功能或替换实现 |
| **可重用性** | 业务逻辑和数据访问可在多个场景复用 |

---

## 入口层 (Program.cs)

### 职责
应用程序的**唯一入口点**,负责初始化、配置和启动整个应用。

### 文件位置
```
ProductDataIngestion/
└── Program.cs
```

### 主要功能

#### 1. 配置管理
```csharp
var configuration = new ConfigurationBuilder()
    .SetBasePath(Directory.GetCurrentDirectory())
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .AddEnvironmentVariables()
    .Build();
```
- 从 `appsettings.json` 读取配置
- 支持环境变量覆盖
- 读取数据库连接字符串和CSV文件路径

#### 2. 依赖注入 & 服务组装
```csharp
// 创建Repository实例
var batchRepository = new BatchRepository(connectionString);
var productRepository = new ProductRepository(connectionString);
var dataRepository = new DataImportRepository(connectionString);

// 创建Service实例
var dataService = new DataImportService(dataRepository);
var ingestService = new IngestService(connectionString, batchRepository, productRepository);
```

#### 3. 业务流程调用
```csharp
string batchId = await ingestService.ProcessCsvFileAsync(
    csvFilePath,
    groupCompanyCd,
    targetEntity
);
```

#### 4. 异常处理
- 捕获并显示所有未处理异常
- 提供友好的错误消息
- 记录详细的堆栈跟踪

### 执行流程
```
启动 → 加载配置 → 验证配置 → 初始化Repository
     → 初始化Service → 执行导入 → 显示结果 → 结束
```

---

## 数据模型层 (Models/)

### 职责
定义系统中所有的**数据结构**和**常量**,不包含业务逻辑。

### 文件结构
```
Models/
├── ErrorCodes.cs           # 错误代码常量
├── ErrorDetail.cs          # 错误详情模型
├── BatchRun.cs             # 批次运行模型
├── RecordError.cs          # 记录错误模型
├── IngestManifest.cs       # 导入清单模型
├── MDataImportSetting.cs   # 导入设置模型
├── MDataImportD.cs         # 导入明细模型
├── MFixedToAttrMap.cs      # 固定属性映射模型
├── MAttrDefinition.cs      # 属性定义模型
├── MCompany.cs             # 公司模型
├── TempProductParsed.cs    # 临时解析产品模型
└── ClProductAttr.cs        # 产品属性模型
```

---

### 📄 ErrorCodes.cs

**用途**: 定义所有错误代码常量,统一错误分类

**设计模式**: 静态常量类

#### 错误分类

| 分类 | 错误代码 | 说明 |
|-----|---------|------|
| **CSV解析错误** | `PARSE_FAILED` | CSV解析失败 |
| | `INVALID_ENCODING` | 字符编码错误 |
| | `ROW_TOO_LARGE` | 行大小超过限制 |
| **数据验证错误** | `MISSING_COLUMN` | 缺少必需列 |
| | `EMPTY_RECORD` | 空记录 |
| | `REQUIRED_FIELD_EMPTY` | 必需字段为空 |
| **类型转换错误** | `CAST_NUM_FAILED` | 数值转换失败 |
| | `CAST_DATE_FAILED` | 日期转换失败 |
| | `CAST_BOOL_FAILED` | 布尔值转换失败 |
| **映射错误** | `MAPPING_NOT_FOUND` | 未找到映射定义 |
| **数据库错误** | `DB_ERROR` | 数据库操作错误 |
| **文件操作错误** | `S3_MOVE_FAILED` | S3文件移动失败 |
| | `LOCAL_MOVE_FAILED` | 本地文件移动失败 |

#### 使用示例
```csharp
throw new IngestException(
    ErrorCodes.PARSE_FAILED,
    "CSV解析失败",
    recordRef: "Row 123"
);
```

---

### 📄 ErrorDetail.cs

**用途**: 错误详情数据模型,结构化存储错误信息

**设计理念**: 关注点分离 - 将错误信息作为独立模型而非异常类的一部分

#### 核心属性

| 属性 | 类型 | 说明 |
|-----|------|------|
| `ErrorCode` | `string` | 错误代码 (使用ErrorCodes常量) |
| `Message` | `string` | 错误消息 |
| `RecordRef` | `string?` | 记录引用 (行号、ID等) |
| `RawFragment` | `string?` | 原始数据片段 (用于调试) |
| `Timestamp` | `DateTime?` | 错误发生时间 |
| `Context` | `Dictionary<string, string>?` | 额外上下文信息 |

#### 构造函数
```csharp
// 基本构造
public ErrorDetail(string errorCode, string message,
                   string? recordRef = null, string? rawFragment = null)

// 完整构造 (自动记录时间戳)
{
    ErrorCode = errorCode;
    Message = message;
    RecordRef = recordRef;
    RawFragment = rawFragment;
    Timestamp = DateTime.UtcNow;
}
```

#### ToString 方法
提供友好的字符串表示:
```
[PARSE_FAILED] CSV解析失败 | RecordRef: Row 123 | RawData: "invalid,data"
```

#### 应用场景
- 异常处理中携带详细错误信息
- 日志记录
- API错误响应
- 错误报告生成

---

### 📄 BatchRun.cs

**用途**: 批次运行记录模型,跟踪每次导入任务的执行状态

**数据库表**: `batch_run`

#### 核心属性

| 属性 | 类型 | 说明 |
|-----|------|------|
| `BatchId` | `string` | 批次唯一标识 (UUID) |
| `FilePath` | `string` | 源文件路径 |
| `GroupCompanyCd` | `string` | 公司代码 |
| `TargetEntity` | `string` | 目标实体类型 (PRODUCT/EVENT等) |
| `TotalRecordCount` | `int` | 总记录数 |
| `SuccessCount` | `int` | 成功记录数 |
| `ErrorCount` | `int` | 失败记录数 |
| `SkipCount` | `int` | 跳过记录数 |
| `Status` | `string` | 批次状态 (PROCESSING/SUCCESS/PARTIAL/FAILED) |
| `StartedAt` | `DateTime` | 开始时间 |
| `EndedAt` | `DateTime?` | 结束时间 |

#### 内部属性 (不序列化到JSON)

| 属性 | 说明 |
|-----|------|
| `IdemKey` | 幂等键 (防止重复处理) |
| `S3Bucket` | S3存储桶名称 |
| `Etag` | 文件ETag (版本标识) |
| `DataKind` | 数据类型 (映射自TargetEntity) |
| `FileKey` | 文件对象键 |
| `CountsJson` | 统计信息JSON |

#### 核心方法

##### UpdateCounts()
更新处理统计:
```csharp
public void UpdateCounts(int total, int success, int error, int skip)
{
    TotalRecordCount = total;
    SuccessCount = success;
    ErrorCount = error;
    SkipCount = skip;
    UpdateCountsJson();  // 同步更新JSON统计
}
```

##### Complete()
标记批次完成:
```csharp
public void Complete()
{
    // 根据结果设置状态
    Status = SuccessCount > 0 && ErrorCount == 0 ? "SUCCESS" :
             SuccessCount > 0 && ErrorCount > 0 ? "PARTIAL" : "FAILED";
    EndedAt = DateTime.Now;
    UpdateCountsJson();
}
```

##### Fail()
标记批次失败:
```csharp
public void Fail()
{
    Status = "FAILED";
    EndedAt = DateTime.Now;
    UpdateCountsJson();
}
```

#### CountsJson 结构
```json
{
  "total": 100,
  "success": 95,
  "error": 5,
  "skip": 0,
  "ingest": {
    "read": 100,
    "ok": 95,
    "ng": 5
  },
  "cleanse": {
    "processed": 95
  },
  "upsert": {
    "processed": 95
  }
}
```

---

### 📄 MDataImportSetting.cs

**用途**: CSV导入配置模型,定义如何解析CSV文件

**数据库表**: `m_data_import_setting`

#### 核心属性

| 属性 | 类型 | 说明 |
|-----|------|------|
| `ProfileId` | `long` | 配置文件ID (主键) |
| `UsageNm` | `string` | 用途名称 (如 "KM-PRODUCT") |
| `GroupCompanyCd` | `string` | 公司代码 |
| `TargetEntity` | `string` | 目标实体 |
| `CharacterCd` | `string` | 字符编码 (UTF-8/Shift_JIS/GBK等) |
| `Delimiter` | `string` | 分隔符 (默认逗号) |
| `HeaderRowIndex` | `int` | 标题行索引 (1-based) |
| `SkipRowCount` | `int` | 跳过行数 |
| `IsActive` | `bool` | 是否启用 |

#### 使用场景
在 `IngestService.ProcessCsvFileAsync()` 中:
```csharp
var importSetting = await _dataService.GetImportSettingAsync(groupCompanyCd, usageNm);

// 配置CSV Reader
var config = new CsvConfiguration(CultureInfo.InvariantCulture)
{
    Delimiter = importSetting.Delimiter ?? ",",
    HasHeaderRecord = importSetting.HeaderRowIndex > 0
};
```

---

### 📄 MDataImportD.cs

**用途**: CSV列映射明细模型,定义每列如何映射到目标字段

**数据库表**: `m_data_import_d`

#### 核心属性

| 属性 | 类型 | 说明 |
|-----|------|------|
| `ProfileId` | `long` | 关联配置文件ID |
| `ColumnSeq` | `int` | 列序号 (从1开始) |
| `ProjectionKind` | `string` | 映射类型 (FIXED/EAV/SKIP) |
| `AttrCd` | `string?` | 属性代码 |
| `TargetColumn` | `string?` | 目标列名 |
| `CastType` | `string?` | 类型转换 (STRING/INT/DATE等) |
| `TransformExpr` | `string?` | 转换表达式 (trim/upper/nullif等) |
| `IsRequired` | `bool` | 是否必需 |

#### ProjectionKind 类型

| 类型 | 说明 | 示例 |
|-----|------|------|
| `FIXED` | 固定列映射 | 产品ID → product_id列 |
| `EAV` | EAV属性映射 | 品牌 → cl_product_attr表 |
| `SKIP` | 跳过该列 | 备注列不导入 |

#### TransformExpr 表达式

| 表达式 | 功能 | 示例 |
|--------|------|------|
| `trim(@)` | 去除前后空格 | "  ABC  " → "ABC" |
| `upper(@)` | 转大写 | "abc" → "ABC" |
| `lower(@)` | 转小写 | "ABC" → "abc" |
| `nullif(@,'')` | 空字符串转null | "" → null |
| `to_timestamp(@,'YYYY-MM-DD')` | 日期解析 | "2025-10-22" → DateTime |

#### 组合表达式
```
trim(@) + upper(@)          →  "  abc  " → "ABC"
trim(@) + nullif(@,'')      →  "   " → null
```

---

### 📄 MAttrDefinition.cs

**用途**: 属性定义模型,定义EAV模型中的属性元数据

**数据库表**: `m_attr_definition`

#### 核心属性

| 属性 | 类型 | 说明 |
|-----|------|------|
| `AttrId` | `long` | 属性ID (主键) |
| `AttrCd` | `string` | 属性代码 (唯一标识) |
| `AttrNm` | `string` | 属性名称 |
| `DataType` | `string` | 数据类型 (STRING/INT/DATE/BOOL等) |
| `GCategoryCd` | `string?` | 大类代码 |
| `IsGoldenAttr` | `bool` | 是否为金数据属性 |
| `TargetTable` | `string?` | 目标表名 |
| `TargetColumn` | `string?` | 目标列名 |
| `IsActive` | `bool` | 是否启用 |

#### 应用场景
在 `AttributeProcessor` 中用于验证属性代码的合法性和确定数据类型。

---

### 📄 TempProductParsed.cs

**用途**: 临时产品解析模型,存储CSV解析后的原始数据

**数据库表**: `temp_product_parsed`

#### 核心属性

| 属性 | 类型 | 说明 |
|-----|------|------|
| `TempRowId` | `string` | 临时行ID (UUID) |
| `BatchId` | `string` | 关联批次ID |
| `RowNumber` | `int` | CSV行号 |
| `ProductId` | `string?` | 产品ID (FIXED列) |
| `ProductCd` | `string?` | 产品代码 (FIXED列) |
| `ExtrasJson` | `string` | 额外数据JSON (EAV列) |
| `RawRow` | `string` | 原始CSV行数据 |
| `Status` | `string` | 状态 (PARSED/VALIDATED/FAILED) |

#### ExtrasJson 结构
存储所有EAV类型的列数据:
```json
{
  "processed_columns": {
    "brand": {
      "column_seq": 3,
      "attr_cd": "BRAND",
      "source_label": "Nike",
      "source_raw": "Nike",
      "data_type": "STRING"
    },
    "price": {
      "column_seq": 4,
      "attr_cd": "PRICE",
      "source_label": "1000",
      "source_raw": "1000",
      "data_type": "INT"
    }
  }
}
```

---

### 📄 ClProductAttr.cs

**用途**: 产品属性EAV模型,存储产品的动态属性

**数据库表**: `cl_product_attr`

#### 核心属性

| 属性 | 类型 | 说明 |
|-----|------|------|
| `BatchId` | `string` | 关联批次ID |
| `TempRowId` | `string` | 关联临时行ID |
| `AttrCd` | `string` | 属性代码 |
| `AttrSeq` | `int` | 属性序号 |
| `SourceId` | `string?` | 源ID值 |
| `SourceLabel` | `string?` | 源标签值 (显示值) |
| `SourceRaw` | `string?` | 源原始值 (未转换) |
| `DataType` | `string` | 数据类型 |

#### EAV模型说明
EAV (Entity-Attribute-Value) 模型允许存储动态属性:

| Entity (实体) | Attribute (属性) | Value (值) |
|-------------|----------------|-----------|
| Product_001 | BRAND | Nike |
| Product_001 | PRICE | 1000 |
| Product_001 | COLOR | Red |

**优势**:
- 灵活添加新属性,无需修改表结构
- 支持稀疏数据 (不是每个产品都有所有属性)
- 易于扩展

---

### 📄 RecordError.cs

**用途**: 记录错误模型,存储导入过程中的错误信息

**数据库表**: `record_error`

#### 核心属性

| 属性 | 类型 | 说明 |
|-----|------|------|
| `ErrorId` | `string` | 错误ID (UUID) |
| `BatchId` | `string` | 关联批次ID |
| `RowNumber` | `int` | 错误发生的行号 |
| `ErrorCode` | `string` | 错误代码 |
| `ErrorMessage` | `string` | 错误消息 |
| `RecordRef` | `string?` | 记录引用 |
| `RawFragment` | `string?` | 原始数据片段 |
| `CreatedAt` | `DateTime` | 创建时间 |

---

## 数据访问层 (Repositories/)

### 职责
封装所有**数据库访问逻辑**,提供统一的数据操作接口。

### 设计原则
- **接口隔离**: 每个Repository定义独立接口
- **依赖倒置**: Service层依赖接口而非实现
- **单一职责**: 每个Repository只负责一个领域实体

### 文件结构
```
Repositories/
├── Interfaces/
│   ├── IBatchRepository.cs         # 批次仓储接口
│   ├── IDataImportRepository.cs    # 导入仓储接口
│   └── IProductRepository.cs       # 产品仓储接口
├── BatchRepository.cs              # 批次仓储实现
├── DataImportRepository.cs         # 导入仓储实现
└── ProductRepository.cs            # 产品仓储实现
```

---

### 📄 IBatchRepository.cs

**职责**: 批次运行数据的CRUD接口定义

#### 接口方法

| 方法 | 说明 |
|-----|------|
| `CreateBatchRunAsync()` | 创建批次记录 |
| `UpdateBatchRunAsync()` | 更新批次统计 |
| `GetBatchRunAsync()` | 查询批次信息 |
| `MarkBatchFailedAsync()` | 标记批次失败 |

---

### 📄 BatchRepository.cs

**实现**: `IBatchRepository` 接口的数据库实现

#### 核心实现: CreateBatchRunAsync

```csharp
public async Task CreateBatchRunAsync(BatchRun batchRun)
{
    using var connection = new NpgsqlConnection(_connectionString);

    var sql = @"
        INSERT INTO batch_run (
            batch_id, idem_key, s3_bucket, etag, group_company_cd,
            data_kind, file_key, batch_status, counts_json,
            started_at, ended_at, cre_at, upd_at
        ) VALUES (
            @BatchId, @IdemKey, @S3Bucket, @Etag, @GroupCompanyCd,
            @DataKind, @FileKey, @BatchStatus, @CountsJson::jsonb,
            @StartedAt, @EndedAt, @CreAt, @UpdAt
        )";

    await connection.ExecuteAsync(sql, batchRun);
}
```

**技术要点**:
- 使用 `Dapper` 进行参数化查询
- `counts_json::jsonb` 类型转换确保JSON格式正确
- 自动管理连接生命周期 (`using` 语句)

---

### 📄 IDataImportRepository.cs

**职责**: 导入配置数据的查询接口定义

#### 接口方法

| 方法 | 说明 |
|-----|------|
| `GetImportSettingAsync()` | 获取导入设置 |
| `GetImportDetailsAsync()` | 获取导入明细 |
| `GetFixedToAttrMapsAsync()` | 获取固定属性映射 |
| `GetAttrDefinitionsAsync()` | 获取属性定义 |

---

### 📄 DataImportRepository.cs

**实现**: `IDataImportRepository` 接口的数据库实现

#### SQL查询常量类

为提高可维护性,所有SQL查询统一定义在 `SqlQueries` 静态类中:

```csharp
static class SqlQueries
{
    public const string GetImportSetting = @"
        SELECT
            profile_id as ProfileId,
            usage_nm as UsageNm,
            group_company_cd as GroupCompanyCd,
            target_entity as TargetEntity,
            character_cd as CharacterCd,
            delimiter as Delimiter,
            header_row_index as HeaderRowIndex,
            skip_row_count as SkipRowCount,
            is_active as IsActive
        FROM m_data_import_setting
        WHERE group_company_cd = @GroupCompanyCd
            AND usage_nm = @UsageNm
            AND is_active = true";
}
```

**优势**:
- 集中管理SQL,便于维护
- 支持多行格式,提高可读性
- 列别名映射到C#属性 (如 `profile_id as ProfileId`)

#### 异常处理

```csharp
public async Task<MDataImportSetting> GetImportSettingAsync(
    string groupCompanyCd, string usageNm)
{
    using var connection = new NpgsqlConnection(_connectionString);
    var setting = await connection.QueryFirstOrDefaultAsync<MDataImportSetting>(
        SqlQueries.GetImportSetting,
        new { GroupCompanyCd = groupCompanyCd, UsageNm = usageNm });

    if (setting == null)
    {
        throw new ImportException(
            $"Import setting not found: GroupCompanyCd={groupCompanyCd}, UsageNm={usageNm}"
        );
    }

    return setting;
}
```

**要点**:
- 找不到配置时抛出 `ImportException`
- 异常消息包含足够的上下文信息

---

### 📄 IProductRepository.cs

**职责**: 产品数据的存储接口定义

#### 接口方法

| 方法 | 说明 |
|-----|------|
| `SaveTempProductAsync()` | 保存临时产品数据 |
| `GetTempProductsAsync()` | 查询临时产品数据 |
| `SaveProductAttributeAsync()` | 保存产品属性(EAV) |
| `SaveProductAttributesBatchAsync()` | 批量保存产品属性 |

---

### 📄 ProductRepository.cs

**实现**: `IProductRepository` 接口的数据库实现

#### 核心实现: SaveProductAttributeAsync

```csharp
public async Task SaveProductAttributeAsync(ClProductAttr attr)
{
    using var connection = new NpgsqlConnection(_connectionString);

    var sql = @"
        INSERT INTO cl_product_attr (
            batch_id, temp_row_id, attr_cd, attr_seq,
            source_id, source_label, source_raw, data_type
        ) VALUES (
            @BatchId, @TempRowId, @AttrCd, @AttrSeq,
            @SourceId, @SourceLabel, @SourceRaw, @DataType
        )";

    await connection.ExecuteAsync(sql, attr);
}
```

#### 批量插入优化

```csharp
public async Task SaveProductAttributesBatchAsync(List<ClProductAttr> attrs)
{
    if (!attrs.Any()) return;

    using var connection = new NpgsqlConnection(_connectionString);
    await connection.OpenAsync();

    using var transaction = await connection.BeginTransactionAsync();
    try
    {
        foreach (var attr in attrs)
        {
            await SaveProductAttributeAsync(attr);
        }
        await transaction.CommitAsync();
    }
    catch
    {
        await transaction.RollbackAsync();
        throw;
    }
}
```

**事务管理**:
- 批量操作使用事务
- 失败时自动回滚
- 确保数据一致性

---

## 业务逻辑层 (Services/)

### 职责
实现所有**业务规则和处理流程**,协调各个Repository完成复杂操作。

### 文件结构
```
Services/
├── IngestException.cs      # Ingest异常类
├── ImportException.cs      # Import异常类
├── IngestService.cs        # 核心导入服务
├── DataImportService.cs    # 数据导入服务
├── AttributeProcessor.cs   # 属性处理器
└── CsvValidator.cs         # CSV验证器
```

---

### 📄 IngestException.cs

**用途**: Ingest处理的自定义异常类

#### 设计特点
- 继承自 `Exception`
- 内部持有 `ErrorDetail` 模型
- 提供便利属性访问错误信息

#### 核心实现

```csharp
public class IngestException : Exception
{
    public ErrorDetail ErrorDetail { get; }

    // 构造函数: ErrorDetail对象
    public IngestException(ErrorDetail errorDetail)
        : base(errorDetail.Message)
    {
        ErrorDetail = errorDetail;
    }

    // 构造函数: 简易版 (下位互换性)
    public IngestException(string errorCode, string message,
                          string? recordRef = null, string? rawFragment = null)
        : base(message)
    {
        ErrorDetail = new ErrorDetail(errorCode, message, recordRef, rawFragment);
    }

    // 便利属性
    public string ErrorCode => ErrorDetail.ErrorCode;
    public string? RecordRef => ErrorDetail.RecordRef;
    public string? RawFragment => ErrorDetail.RawFragment;
}
```

#### 使用示例

```csharp
// 方式1: 使用ErrorDetail对象
var errorDetail = new ErrorDetail(
    ErrorCodes.PARSE_FAILED,
    "CSV解析失败",
    recordRef: "Row 123",
    rawFragment: "invalid,data,line"
);
throw new IngestException(errorDetail);

// 方式2: 简易构造 (推荐)
throw new IngestException(
    ErrorCodes.MISSING_COLUMN,
    "缺少必需列: product_id",
    recordRef: "Header Row"
);
```

---

### 📄 ImportException.cs

**用途**: Import处理的自定义异常类

#### 与IngestException的区别

| 异常类 | 使用场景 |
|--------|---------|
| `IngestException` | CSV导入业务流程中的错误 |
| `ImportException` | 导入配置、数据库访问等基础错误 |

#### 核心实现

```csharp
public class ImportException : Exception
{
    public ErrorDetail ErrorDetail { get; }

    // 简易构造 (仅消息)
    public ImportException(string message)
        : base(message)
    {
        ErrorDetail = new ErrorDetail(ErrorCodes.DB_ERROR, message);
    }

    // 带内部异常
    public ImportException(string message, Exception innerException)
        : base(message, innerException)
    {
        ErrorDetail = new ErrorDetail(ErrorCodes.DB_ERROR, message);
    }
}
```

---

### 📄 IngestService.cs

**用途**: **核心业务服务**,负责整个CSV导入流程的编排

#### 依赖注入

```csharp
public class IngestService
{
    private readonly DataImportService _dataService;
    private readonly IBatchRepository _batchRepository;
    private readonly IProductRepository _productRepository;
    private readonly CsvValidator _csvValidator;
    private readonly AttributeProcessor _attributeProcessor;

    public IngestService(
        string connectionString,
        IBatchRepository batchRepository,
        IProductRepository productRepository)
    {
        // 初始化所有依赖
    }
}
```

#### 主流程: ProcessCsvFileAsync()

**完整业务流程** (10个步骤):

```
┌─────────────────────────────────────────┐
│  1. 验证公司代码                         │
└────────────┬────────────────────────────┘
             │
┌────────────┴────────────────────────────┐
│  2. 创建批次记录 (BatchRun)              │
└────────────┬────────────────────────────┘
             │
┌────────────┴────────────────────────────┐
│  3. 获取导入规则                         │
│     - MDataImportSetting               │
│     - MDataImportD (列映射)            │
└────────────┬────────────────────────────┘
             │
┌────────────┴────────────────────────────┐
│  4. 配置CSV Reader                      │
│     - 字符编码                          │
│     - 分隔符                            │
│     - 标题行位置                        │
└────────────┬────────────────────────────┘
             │
┌────────────┴────────────────────────────┐
│  5. 逐行读取CSV                         │
│     - 跳过无效行                        │
│     - 应用转换表达式                    │
└────────────┬────────────────────────────┘
             │
┌────────────┴────────────────────────────┐
│  6. 数据验证                             │
│     - 必需字段检查                       │
│     - 类型转换验证                       │
└────────────┬────────────────────────────┘
             │
┌────────────┴────────────────────────────┐
│  7. 保存到临时表                         │
│     (temp_product_parsed)              │
└────────────┬────────────────────────────┘
             │
┌────────────┴────────────────────────────┐
│  8. 从extras_json提取EAV属性            │
└────────────┬────────────────────────────┘
             │
┌────────────┴────────────────────────────┐
│  9. 保存产品属性                         │
│     (cl_product_attr)                  │
└────────────┬────────────────────────────┘
             │
┌────────────┴────────────────────────────┐
│  10. 更新批次统计                        │
│      - 总数/成功/失败/跳过              │
│      - 批次状态                         │
└─────────────────────────────────────────┘
```

#### 核心方法详解

##### 1. ValidateCompanyCodeAsync()

```csharp
private async Task ValidateCompanyCodeAsync(string groupCompanyCd)
{
    // 查询数据库验证公司代码
    var company = await GetCompanyAsync(groupCompanyCd);
    if (company == null)
    {
        throw new IngestException(
            ErrorCodes.DB_ERROR,
            $"无效的公司代码: {groupCompanyCd}"
        );
    }
}
```

##### 2. CreateBatchRunAsync()

```csharp
private async Task<string> CreateBatchRunAsync(
    string filePath, string groupCompanyCd, string targetEntity)
{
    var batchId = Guid.NewGuid().ToString();
    var batchRun = new BatchRun
    {
        BatchId = batchId,
        FilePath = filePath,
        GroupCompanyCd = groupCompanyCd,
        TargetEntity = targetEntity,
        Status = "PROCESSING",
        StartedAt = DateTime.Now
    };

    await _batchRepository.CreateBatchRunAsync(batchRun);
    return batchId;
}
```

##### 3. FetchImportRulesAsync()

```csharp
private async Task<(MDataImportSetting, List<MDataImportD>)>
    FetchImportRulesAsync(string groupCompanyCd, string targetEntity)
{
    var usageNm = $"{groupCompanyCd}-{targetEntity}";

    // 获取导入设置
    var setting = await _dataService.GetImportSettingAsync(
        groupCompanyCd, usageNm
    );

    // 获取列映射明细
    var details = await _dataService.GetImportDetailsAsync(
        setting.ProfileId
    );

    return (setting, details);
}
```

##### 4-7. ReadCsvAndSaveToTempAsync()

**最复杂的方法**,负责CSV读取、转换、验证和保存:

```csharp
private async Task<(int readCount, int okCount, int ngCount)>
    ReadCsvAndSaveToTempAsync(
        string filePath,
        string batchId,
        string groupCompanyCd,
        int headerRowIndex,
        List<MDataImportD> importDetails,
        CsvConfiguration config)
{
    int readCount = 0, okCount = 0, ngCount = 0;

    using var reader = new StreamReader(filePath, GetEncoding());
    using var csv = new CsvReader(reader, config);

    // 跳过标题行之前的行
    for (int i = 1; i < headerRowIndex; i++)
    {
        await csv.ReadAsync();
    }

    // 读取标题行
    await csv.ReadAsync();
    csv.ReadHeader();

    // 逐行处理数据
    while (await csv.ReadAsync())
    {
        readCount++;
        try
        {
            // 提取FIXED列
            var productId = GetFieldValue(csv, "product_id", importDetails);
            var productCd = GetFieldValue(csv, "product_cd", importDetails);

            // 提取EAV列到extras_json
            var extrasJson = BuildExtrasJson(csv, importDetails);

            // 创建临时记录
            var tempProduct = new TempProductParsed
            {
                TempRowId = Guid.NewGuid().ToString(),
                BatchId = batchId,
                RowNumber = readCount,
                ProductId = productId,
                ProductCd = productCd,
                ExtrasJson = extrasJson,
                RawRow = GetRawRow(csv),
                Status = "PARSED"
            };

            // 保存到数据库
            await _productRepository.SaveTempProductAsync(tempProduct);
            okCount++;
        }
        catch (IngestException ex)
        {
            // 记录错误
            await RecordErrorAsync(batchId, readCount, ex);
            ngCount++;
        }
    }

    return (readCount, okCount, ngCount);
}
```

**关键技术**:
- 使用 `CsvHelper` 库解析CSV
- 支持自定义字符编码
- 流式读取,内存占用低
- 异常捕获不中断处理

##### 8-9. GenerateProductAttributesAsync()

从 `temp_product_parsed.extras_json` 提取数据并生成EAV属性:

```csharp
private async Task GenerateProductAttributesAsync(
    string batchId, string groupCompanyCd, string targetEntity)
{
    // 获取所有临时记录
    var tempProducts = await _productRepository.GetTempProductsAsync(batchId);

    foreach (var product in tempProducts)
    {
        // 解析extras_json
        var processedColumns = _attributeProcessor
            .ExtractProcessedColumns(product.ExtrasJson);

        int attrSeq = 1;
        var attrs = new List<ClProductAttr>();

        foreach (var (columnName, columnInfo) in processedColumns)
        {
            var attr = new ClProductAttr
            {
                BatchId = batchId,
                TempRowId = product.TempRowId,
                AttrCd = columnInfo.AttrCd,
                AttrSeq = attrSeq++,
                SourceId = columnInfo.SourceId,
                SourceLabel = columnInfo.SourceLabel,
                SourceRaw = columnInfo.SourceRaw,
                DataType = columnInfo.DataType
            };
            attrs.Add(attr);
        }

        // 批量保存
        await _productRepository.SaveProductAttributesBatchAsync(attrs);
    }
}
```

##### 10. UpdateBatchStatisticsAsync()

```csharp
private async Task UpdateBatchStatisticsAsync(
    string batchId, (int read, int ok, int ng) result)
{
    var batchRun = await _batchRepository.GetBatchRunAsync(batchId);
    batchRun.UpdateCounts(
        total: result.read,
        success: result.ok,
        error: result.ng,
        skip: 0
    );
    batchRun.Complete();

    await _batchRepository.UpdateBatchRunAsync(batchRun);
}
```

#### 异常处理策略

```csharp
try
{
    // 主流程
}
catch (IngestException ex)
{
    // 业务异常 - 记录并标记批次失败
    await RecordErrorAndMarkBatchFailed(batchId, ex);
    throw;
}
catch (Exception ex)
{
    // 未知异常 - 包装为IngestException
    var wrappedException = new IngestException(
        ErrorCodes.DB_ERROR,
        $"未知错误: {ex.Message}",
        ex
    );
    await RecordErrorAndMarkBatchFailed(batchId, wrappedException);
    throw wrappedException;
}
```

---

### 📄 DataImportService.cs

**用途**: 数据导入服务,提供配置查询和CSV读取功能

#### 接口定义

```csharp
public interface IDataImportService
{
    Task<MDataImportSetting> GetImportSettingAsync(string groupCompanyCd, string usageNm);
    Task<List<MDataImportD>> GetImportDetailsAsync(long profileId);
    Task<List<MFixedToAttrMap>> GetFixedToAttrMapsAsync(string groupCompanyCd, string dataKind);
    Task<List<MAttrDefinition>> GetAttrDefinitionsAsync();
    Task<List<T>> ReadCsvWithSettingsAsync<T>(string filePath, MDataImportSetting setting) where T : class, new();
    Task<List<string[]>> ReadCsvRawAsync(string filePath, MDataImportSetting setting);
}
```

#### 核心实现: ReadCsvRawAsync()

```csharp
public async Task<List<string[]>> ReadCsvRawAsync(
    string filePath, MDataImportSetting setting)
{
    var rawData = new List<string[]>();

    using var reader = new StreamReader(
        filePath,
        GetEncoding(setting.CharacterCd ?? "UTF-8")
    );
    using var csv = new CsvReader(
        reader,
        GetCsvConfiguration(setting)
    );

    await SkipRowsAsync(csv, setting);

    while (await csv.ReadAsync())
    {
        var record = new List<string>();
        for (int i = 0; csv.TryGetField<string>(i, out string? field); i++)
        {
            record.Add(field ?? string.Empty);
        }
        rawData.Add(record.ToArray());
    }

    return rawData;
}
```

#### 字符编码支持

```csharp
private static Encoding GetEncoding(string characterCd)
{
    return characterCd?.ToUpperInvariant() switch
    {
        "UTF-8" => Encoding.UTF8,
        "SHIFT_JIS" => Encoding.GetEncoding("Shift_JIS"),
        "EUC-JP" => Encoding.GetEncoding("EUC-JP"),
        "GBK" => Encoding.GetEncoding("GBK"),
        _ => Encoding.UTF8
    };
}
```

---

### 📄 AttributeProcessor.cs

**用途**: 属性处理器,负责从 `extras_json` 提取并处理EAV属性

#### 核心方法: ExtractProcessedColumns()

```csharp
public Dictionary<string, ProcessedColumnInfo> ExtractProcessedColumns(
    string extrasJson)
{
    try
    {
        var extrasRoot = JsonSerializer.Deserialize<Dictionary<string, JsonElement>>(
            extrasJson
        );

        if (extrasRoot == null || !extrasRoot.ContainsKey("processed_columns"))
        {
            return new Dictionary<string, ProcessedColumnInfo>();
        }

        var processedColumns = new Dictionary<string, ProcessedColumnInfo>();
        var processedColumnsElement = extrasRoot["processed_columns"];

        foreach (var property in processedColumnsElement.EnumerateObject())
        {
            var columnInfo = JsonSerializer.Deserialize<ProcessedColumnInfo>(
                property.Value.GetRawText()
            );
            if (columnInfo != null)
            {
                processedColumns[property.Name] = columnInfo;
            }
        }

        return processedColumns;
    }
    catch (Exception ex)
    {
        throw new IngestException(
            ErrorCodes.PARSE_FAILED,
            $"extras_json解析失败: {ex.Message}",
            ex
        );
    }
}
```

#### ProcessedColumnInfo 模型

```csharp
public class ProcessedColumnInfo
{
    public int ColumnSeq { get; set; }
    public string AttrCd { get; set; } = string.Empty;
    public string? SourceId { get; set; }
    public string? SourceLabel { get; set; }
    public string? SourceRaw { get; set; }
    public string DataType { get; set; } = "STRING";
}
```

---

### 📄 CsvValidator.cs

**用途**: CSV验证器,验证数据完整性和格式正确性

#### 核心方法

##### ValidateRequiredFields()

```csharp
public void ValidateRequiredFields(
    CsvReader csv,
    List<MDataImportD> importDetails,
    int rowNumber)
{
    foreach (var detail in importDetails.Where(d => d.IsRequired))
    {
        var value = GetFieldValue(csv, detail.TargetColumn);

        if (string.IsNullOrWhiteSpace(value))
        {
            throw new IngestException(
                ErrorCodes.REQUIRED_FIELD_EMPTY,
                $"必需字段为空: {detail.TargetColumn}",
                recordRef: $"Row {rowNumber}"
            );
        }
    }
}
```

##### ValidateDataType()

```csharp
public void ValidateDataType(string value, string castType, int rowNumber)
{
    try
    {
        switch (castType?.ToUpper())
        {
            case "INT":
                int.Parse(value);
                break;
            case "DECIMAL":
                decimal.Parse(value);
                break;
            case "DATE":
                DateTime.Parse(value);
                break;
            case "BOOL":
                bool.Parse(value);
                break;
        }
    }
    catch (FormatException)
    {
        var errorCode = castType?.ToUpper() switch
        {
            "INT" => ErrorCodes.CAST_NUM_FAILED,
            "DECIMAL" => ErrorCodes.CAST_NUM_FAILED,
            "DATE" => ErrorCodes.CAST_DATE_FAILED,
            "BOOL" => ErrorCodes.CAST_BOOL_FAILED,
            _ => ErrorCodes.PARSE_FAILED
        };

        throw new IngestException(
            errorCode,
            $"类型转换失败: 无法将 '{value}' 转换为 {castType}",
            recordRef: $"Row {rowNumber}",
            rawFragment: value
        );
    }
}
```

---

## 数据流程

### 完整数据流

```
CSV文件
  │
  ├─→ [1] 读取配置 (m_data_import_setting, m_data_import_d)
  │
  ├─→ [2] 解析CSV (按配置的编码、分隔符)
  │
  ├─→ [3] 应用转换表达式 (trim, upper, nullif, etc.)
  │
  ├─→ [4] 数据验证 (必需字段、类型转换)
  │
  ├─→ [5] 分类数据
  │       ├─ FIXED列 → product_id, product_cd 等
  │       └─ EAV列 → extras_json
  │
  ├─→ [6] 保存临时数据 (temp_product_parsed)
  │
  ├─→ [7] 从extras_json提取属性
  │
  ├─→ [8] 生成EAV记录 (cl_product_attr)
  │
  └─→ [9] 更新批次统计 (batch_run)
```

### 数据库表关系

```
batch_run (批次记录)
   │
   ├─→ temp_product_parsed (临时产品数据)
   │      │
   │      └─→ cl_product_attr (产品属性EAV)
   │
   └─→ record_error (错误记录)
```

---

## 错误处理机制

### 异常层次结构

```
Exception (基类)
   │
   ├─→ IngestException (业务异常)
   │      ├─ 包含 ErrorDetail 模型
   │      ├─ 错误代码分类
   │      └─ 上下文信息
   │
   └─→ ImportException (基础设施异常)
          ├─ 配置错误
          └─ 数据库错误
```

### 错误处理流程

```
┌─────────────────┐
│  异常发生        │
└────────┬────────┘
         │
    ┌────┴─────┐
    │ 捕获异常  │
    └────┬─────┘
         │
    ┌────┴──────────────┐
    │ 记录到record_error │
    └────┬──────────────┘
         │
    ┌────┴────────────┐
    │ 更新批次状态     │
    │ (FAILED/PARTIAL)│
    └────┬────────────┘
         │
    ┌────┴──────┐
    │ 继续/中断  │
    └───────────┘
```

### 错误记录示例

```csharp
private async Task RecordErrorAsync(
    string batchId, int rowNumber, IngestException ex)
{
    var error = new RecordError
    {
        ErrorId = Guid.NewGuid().ToString(),
        BatchId = batchId,
        RowNumber = rowNumber,
        ErrorCode = ex.ErrorCode,
        ErrorMessage = ex.Message,
        RecordRef = ex.RecordRef,
        RawFragment = ex.RawFragment,
        CreatedAt = DateTime.UtcNow
    };

    await _batchRepository.SaveRecordErrorAsync(error);
}
```

---

## 总结

### 三层架构优势总结

| 层 | 职责 | 优势 |
|----|-----|------|
| **入口层** | 应用启动、配置管理 | 统一入口,易于部署 |
| **业务逻辑层** | 流程编排、规则实现 | 业务逻辑集中,易于维护 |
| **数据访问层** | 数据库操作封装 | 数据访问统一,易于测试 |

### 关键设计模式

1. **Repository模式**: 封装数据访问逻辑
2. **依赖注入**: 降低耦合度,提高可测试性
3. **异常处理模式**: 统一错误处理机制
4. **EAV模式**: 支持动态属性扩展

### 扩展性

系统设计支持以下扩展:

- ✅ 添加新的数据源类型 (Excel, XML等)
- ✅ 添加新的转换表达式
- ✅ 添加新的验证规则
- ✅ 支持更多字符编码
- ✅ 集成消息队列进行异步处理
- ✅ 添加数据清洗步骤

---

**文档版本**: 1.0
**最后更新**: 2025-10-22
**维护者**: Claude Code
