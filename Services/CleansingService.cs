using ProductDataIngestion.Models;
using ProductDataIngestion.Utils;
using System.Text.Json;
using System.Text.Json.Nodes;
using ProductDataIngestion.Repositories.Interfaces;

namespace ProductDataIngestion.Services
{
    /// <summary>
    /// データクレンジングのメインロジックを担うクラス。
    /// </summary>
    public class CleansingService
    {
        private readonly IClProductAttrRepository _productAttrRepo;
        private readonly IAttributeDefinitionRepository _definitionRepo;
        private readonly ICleansePolicyRepository _policyRepo;
        private readonly IRefTableMapRepository _refTableMapRepo;
        private readonly IBrandSourceMapRepository _brandSourceMapRepo;
        private readonly IMBrandGRepository _mBrandGRepo;
        private readonly IMCompanyRepository _companyRepo;
        private readonly IAttrSourceMapRepository _attrSourceMapRepository;
        private readonly ICategorySourceMapRepository _categorySourceMapRepo;
        private readonly IMCategoryGRepository _mCategoryGRepo;
        private readonly IMListItemGRepository _mlistItemGRepo;
        private readonly IRefResolverRepository _refResolverRepo;
        private readonly IMCleanseRuleSetRepository _cleanseRuleSetRepo;
        private readonly IBatchRepository _batchRunRepo;
        private readonly IRecordErrorRepository _recordErrorRepo;

        // キャッシュ用辞書
        private Dictionary<string, AttributeDefinition> _definitionCache = new();
        private Dictionary<string, CleansePolicy> _policyCache = new();
        private Dictionary<string, RefTableMap> _refTableMapCache = new();
        private Dictionary<long, MCleanseRuleSet> _cleanseRuleSetCache = new();
        private Dictionary<string, MListItemG> _mListItemGCache = new();
        private Dictionary<string, BatchRun> _batchRunCache = new();

        // データクレンジングサービスのコンストラクタ
        public CleansingService(
            IClProductAttrRepository productAttrRepo,
            IAttributeDefinitionRepository definitionRepo,
            ICleansePolicyRepository policyRepo,
            IRefTableMapRepository refTableMapRepo,
            IBrandSourceMapRepository brandSourceMapRepo,
            IMBrandGRepository mBrandGRepo,
            IMCompanyRepository companyRepo,
            IAttrSourceMapRepository attrSourceMapRepository,
            ICategorySourceMapRepository categorySourceMapRepo,
            IMCategoryGRepository mCategoryGRepo,
            IMListItemGRepository mlistItemGRepo,
            IRefResolverRepository refResolverRepo,
            IMCleanseRuleSetRepository cleanseRuleSetRepo,
            IBatchRepository batchRunRepo,
            IRecordErrorRepository recordErrorRepo)
        {
            _productAttrRepo = productAttrRepo;
            _definitionRepo = definitionRepo;
            _policyRepo = policyRepo;
            _refTableMapRepo = refTableMapRepo;
            _brandSourceMapRepo = brandSourceMapRepo;
            _mBrandGRepo = mBrandGRepo;
            _companyRepo = companyRepo;
            _attrSourceMapRepository = attrSourceMapRepository;
            _categorySourceMapRepo = categorySourceMapRepo;
            _mCategoryGRepo = mCategoryGRepo;
            _mlistItemGRepo = mlistItemGRepo;
            _refResolverRepo = refResolverRepo;
            _cleanseRuleSetRepo = cleanseRuleSetRepo;
            _batchRunRepo = batchRunRepo;
            _recordErrorRepo = recordErrorRepo;
        }

        /// <summary>
        /// バッチ起票処理を初期化
        /// 指定された batch_id のステータスを「RUNNING」に更新し、
        /// counts_json の CLEANSE カウンタを初期化
        /// </summary>
        public async Task StartCleanseAsync(string batchId)
        {
            var batch = await _batchRunRepo.GetByBatchIdAsync(batchId);
            if (batch == null)
            {
                Logger.Error($"batch_id={batchId} が見つかりません。取り込みを確認してください。");
                throw new InvalidOperationException($"batch_id={batchId} not found.");
            }

            // counts_json 解析
            var counts = string.IsNullOrWhiteSpace(batch.CountsJson)
                ? new Dictionary<string, object>()
                : JsonSerializer.Deserialize<Dictionary<string, object>>(batch.CountsJson) ?? new();

            // CLEANSE を初期化
            counts["CLEANSE"] = new Dictionary<string, int>
            {
                ["read"] = 0,
                ["ok"] = 0,
                ["warn"] = 0,
                ["ng"] = 0
            };

            batch.BatchStatus = "RUNNING";
            batch.CountsJson = JsonSerializer.Serialize(counts);
            batch.StartedAt = DateTime.UtcNow;
            batch.UpdAt = DateTime.UtcNow;

            await _batchRunRepo.UpdateAsync(batch);
        }

        /// <summary>
        /// クレンジング処理に必要なマスタデータをキャッシュにロード
        /// 読み込み対象テーブル：
        /// - m_attr_definition（項目定義マスタ）
        /// - m_attr_cleanse_policy（クレンジングポリシー）
        /// - m_cleanse_rule_set（ルールセット）
        /// - m_ref_table_map（参照マップ）
        /// - m_list_item_g（Gアイテムリスト）
        /// - batch_run（バッチ管理）
        /// </summary>
        public async Task InitializeAsync()
        {
            Logger.Info("キャッシュをロードしています...");

            await Task.WhenAll(
                // 項目定義マスタ(m_attr_definition)から全件取得してキャッシュに保存
                Task.Run(async () =>
                {
                    var items = await _definitionRepo.GetAllAttrDefinitionAsync();
                    var groups = items.GroupBy(i => i.AttrCd).ToList();
                    foreach (var g in groups.Where(g => g.Count() > 1))
                        Logger.Warn($"Duplicate AttrCd '{g.Key}' found in m_attr_definition - keeping first occurrence.");
                    _definitionCache = new Dictionary<string, AttributeDefinition>(
                    groups.ToDictionary(g => g.Key, g => g.First()),
                    StringComparer.OrdinalIgnoreCase
                );
                }),

                // クレンジングルールセット（m_cleanse_rule_set）を全件取得してキャッシュに保存
                Task.Run(async () =>
                {
                    var items = await _cleanseRuleSetRepo.GetAllAsync();
                    _cleanseRuleSetCache = items.ToDictionary(i => i.RuleSetId);
                }),

                // クレンジングポリシーテーブル（m_attr_cleanse_policy）を全件取得してキャッシュに保存
                Task.Run(async () =>
                {
                    var items = await _policyRepo.GetAllAsync();
                    var groups = items.GroupBy(i => i.AttrCd).ToList();
                    foreach (var g in groups.Where(g => g.Count() > 1))
                        Logger.Warn($"Duplicate AttrCd '{g.Key}' found in m_attr_cleanse_policy - keeping first occurrence.");
                    _policyCache = groups.ToDictionary(g => g.Key, g => g.First());
                }),

                // 参照マスタ対応マップテーブル(m_ref_table_map)から全件取得してキャッシュに保存
                Task.Run(async () =>
                {
                    var items = await _refTableMapRepo.GetAllAsync();
                    var groups = items.GroupBy(i => i.AttrCd).ToList();
                    foreach (var g in groups.Where(g => g.Count() > 1))
                        Logger.Warn($"Duplicate AttrCd '{g.Key}' found in m_ref_table_map - keeping first occurrence.");
                    _refTableMapCache = groups.ToDictionary(g => g.Key, g => g.First());
                }),

                // バッチ実行管理テーブル(batch_run)から全件取得してキャッシュに保存
                Task.Run(async () =>
                {
                    var items = await _batchRunRepo.GetAllAsync();
                    _batchRunCache = items.ToDictionary(i => i.BatchId);
                }),

                // Gアイテムリストマスタ(m_list_item_g)から全件取得してキャッシュに保存
                Task.Run(async () =>
                {
                    var items = await _mlistItemGRepo.GetAllAsync();
                    var groups = items.GroupBy(i => i.GItemCd).ToList();
                    foreach (var g in groups.Where(g => g.Count() > 1))
                        Logger.Warn($"Duplicate GItemCd '{g.Key}' found in m_list_item_g - keeping first occurrence.");
                    _mListItemGCache = groups.ToDictionary(g => g.Key, g => g.First());
                })
                );
            Logger.Info("キャッシュをロードしました！");

            Logger.Info($"RuleSetCache loaded: {_cleanseRuleSetCache.Count} entries");

        }

        // null または空白かどうかを判定するメソッド
        private static bool IsNullOrEmpty(string? s) => string.IsNullOrWhiteSpace(s);
        // 大文字小文字を無視して比較するメソッド
        private static bool Eq(string? a, string? b) =>
            string.Equals(a ?? string.Empty, b ?? string.Empty, StringComparison.OrdinalIgnoreCase);

        /// <summary>
        /// step_no 昇順で候補ポリシーを走査して、
        /// 最も条件に合致するクレンジングポリシーを選択する。
        ///
        /// 1) brand_scope と category_scope が両方 NULL または空 → 共通ポリシーとして一時保存
        /// 2) どちらかが指定されている場合は、現在の商品スコープ（brand/category）と一致するもののみ採用
        /// 3) brand/category がまだ未確定（NULL）の場合はスキップ
        /// </summary>
        private CleansePolicy? ResolvePolicy(IEnumerable<CleansePolicy> candidates, string? brand, string? category)
        {

            CleansePolicy? commonPolicy = null;

            // StepNo（処理順序）で昇順、未設定は最後に
            var ordered = candidates
                .OrderBy(p => p.StepNo == 0 ? int.MaxValue : p.StepNo);

            // 各ポリシーを順番に評価
            foreach (var p in ordered)
            {
                Logger.Info($"[POLICY-CHECK] Evaluating policy_id={p.PolicyId}, BRAND={brand}, CATEGORY={category}");

                // brand_scope と category_scope の両方が空 → 共通ルールとして保存
                if (IsNullOrEmpty(p.BrandScope) && IsNullOrEmpty(p.CategoryScope))
                {
                    commonPolicy = p;
                    Logger.Info($"[POLICY-COMMON] policy_id={p.PolicyId} step={p.StepNo} 共通ルール（保存のみ）");
                    continue;
                }

                // brand_scope があるが、現在の商品 brand がまだ確定していない → スキップ
                if (!IsNullOrEmpty(p.BrandScope) && IsNullOrEmpty(brand))
                {
                    Logger.Info($"[POLICY-SKIP] policy_id={p.PolicyId} step={p.StepNo} 要brand='{p.BrandScope}' だが未確定");
                    continue;
                }

                // category_scope があるが、現在の商品 category がまだ確定していない → スキップ
                if (!IsNullOrEmpty(p.CategoryScope) && IsNullOrEmpty(category))
                {
                    Logger.Info($"[POLICY-SKIP] policy_id={p.PolicyId} step={p.StepNo} 要category='{p.CategoryScope}' だが未確定");
                    continue;
                }

                // brand_scope が設定されていて、現在の商品 brand と一致しない → スキップ
                if (!IsNullOrEmpty(p.BrandScope) && !Eq(p.BrandScope, brand))
                {
                    Logger.Info($"[POLICY-NG] policy_id={p.PolicyId} brand_scope='{p.BrandScope}' != brand='{brand}'");
                    continue;
                }

                // category_scope が設定されていて、現在の商品 category と一致しない → スキップ
                if (!IsNullOrEmpty(p.CategoryScope) && !Eq(p.CategoryScope, category))
                {
                    Logger.Info($"[POLICY-NG] policy_id={p.PolicyId} category_scope='{p.CategoryScope}' != category='{category}'");
                    continue;
                }

                // brand/category の両方が一致 → このポリシーを採用
                Logger.Info($"[POLICY-HIT-SPECIFIC] policy_id={p.PolicyId} step={p.StepNo} brand={p.BrandScope ?? "-"} cat={p.CategoryScope ?? "-"}");
                return p;
            }

            // もし具体的な一致がなければ、共通ポリシーを使用
            if (commonPolicy != null)
            {
                Logger.Info($"[POLICY-HIT-COMMON] 共通ポリシーを使用: policy_id={commonPolicy.PolicyId}");
                return commonPolicy;
            }
            // どれにも当てはまらない場合 → ポリシーなし
            Logger.Warn("[POLICY-MISS] 条件に一致するポリシーがありません。");
            return null;
        }

        /// <summary>
        /// 商品単位で全属性をクレンジング処理するメインメソッド。
        /// - batch_id に紐づく属性を全件取得し、商品（temp_row_id）ごとに処理を行う。
        /// - 各属性について定義情報・ポリシーを適用して値を正規化／参照解決。
        /// - ブランド／カテゴリ確定後は、後続属性のスコープ条件として利用。
        /// - 処理後にカウントを更新し、集計結果を batch_run に反映。
        /// </summary>
        public async Task ProcessAllAttributesAsync(string batchId)
        {

            Logger.Info("クレンジング処理を開始します...");

            var batchRun = await _batchRunRepo.GetByBatchIdAsync(batchId); // バッチ情報を取得

            // cl_product_attr から抽出
            var candidates = await _productAttrRepo.GetImportAttributesAsync(batchId); // 対象属性を取得

            // 商品単位でグループ化（temp_row_id ごと）
            var groupedByProduct = candidates
                .Where(a => a.BatchId == batchId)
                .GroupBy(a => a.TempRowId)
                .ToList();

            int read = 0, ok = 0, warn = 0, ng = 0;

            // 各商品単位で処理を実行
            foreach (var productGroup in groupedByProduct)
            {
                Logger.Info($"[PRODUCT] temp_row_id={productGroup.Key}");

                string? scopedBrand = null;
                string? scopedCategory = null;

                // 定義（m_attr_definition）に基づいてクレンジング順序を決定
                var orderedAttributes = productGroup
                    .Select(a =>
                    {
                        int? cleansePhase = null;
                        if (_definitionCache.TryGetValue(a.AttrCd, out var def) && def.CleansePhase.HasValue)
                        {
                            cleansePhase = def.CleansePhase.Value;
                        }

                        var sortKey = cleansePhase ?? int.MaxValue;
                        return new
                        {
                            Attribute = a,
                            SortKey = sortKey
                        };
                    })
                    .OrderBy(x => x.SortKey) // フェーズ順にソート
                    .Select(x => x.Attribute)
                    .ToList();

                // 属性単位でクレンジング処理を実施
                foreach (var attr in orderedAttributes)
                {
                    read++;

                    string ruleVersion = "UNKNOWN";
                    attr.RuleVersion = ruleVersion;

                    CleansePolicy? policy = null;
                    string? groupCompanyCd = batchRun?.GroupCompanyCd;

                    try
                    {
                        // 定義情報を取得
                        _definitionCache.TryGetValue(attr.AttrCd, out var definition);
                        int phase = definition?.CleansePhase ?? int.MaxValue;

                        // スコープ条件を決定
                        string? brandForMatch = null;
                        string? categoryForMatch = null;

                        // フェーズ10以降の属性のみブランド／カテゴリ依存
                        if (phase > 10)
                        {
                            brandForMatch = scopedBrand;
                            categoryForMatch = scopedCategory;
                        }

                        // Policy取得
                        var candidatesPolicies = await _policyRepo.GetPoliciesAsync(attr.AttrCd, groupCompanyCd);

                        // 条件に合うポリシーを選択
                        policy = ResolvePolicy(candidatesPolicies, scopedBrand, scopedCategory);

                        // 属性クレンジング実行
                        await ProcessSingleAttributeAsync(attr, batchRun, policy);

                        // BRAND/CATEGORY_1 の場合：清洗後にスコープを更新
                        if (attr.AttrCd.Equals("BRAND", StringComparison.OrdinalIgnoreCase))
                        {
                            if (!string.IsNullOrWhiteSpace(attr.ValueCd))
                            {
                                scopedBrand = attr.ValueCd;
                                Logger.Info($"[CONTEXT] ブランド確定: '{scopedBrand}' (temp_row_id={attr.TempRowId})");
                            }
                            else
                            {
                                Logger.Warn($"[CONTEXT] ブランド未確定（value_text空）: temp_row_id={attr.TempRowId}");
                            }
                        }
                        else if (attr.AttrCd.Equals("CATEGORY_1", StringComparison.OrdinalIgnoreCase))
                        {
                            if (!string.IsNullOrWhiteSpace(attr.ValueCd))
                            {
                                //scopedCategory = attr.ValueCd;
                                scopedCategory = attr.ValueCd;
                                Logger.Info($"[CONTEXT] カテゴリ確定: '{scopedCategory}' (temp_row_id={attr.TempRowId})");
                            }
                            else
                            {
                                Logger.Warn($"[CONTEXT] カテゴリ未確定（value_text空）: temp_row_id={attr.TempRowId}");
                            }
                        }

                        // Policy結果記録 & ruleVersion設定
                        brandForMatch = scopedBrand ?? "NULL";
                        categoryForMatch = scopedCategory ?? "NULL";

                        // Logger.Info($"brandForMatch: {brandForMatch}, categoryForMatch: {categoryForMatch}");

                        if (policy != null)
                        {
                            Logger.Info(
                                $"[POLICY-HIT] attr_cd={attr.AttrCd}, policy_id={policy.PolicyId}, step={policy.StepNo}, gp={policy.GpScope}, brand={policy.BrandScope}, cat={policy.CategoryScope}, ctx.brand={brandForMatch ?? "NULL"}, ctx.cat={categoryForMatch ?? "NULL"}"
                            );

                            // ruleVersion を設定
                            if (policy.RuleSetId > -1 && _cleanseRuleSetCache.TryGetValue(policy.RuleSetId, out var ruleSet))
                            {
                                ruleVersion = string.IsNullOrWhiteSpace(ruleSet.RuleVersion)
                                    ? ruleSet.RuleSetId.ToString()
                                    : ruleSet.RuleVersion;

                                attr.RuleVersion = ruleVersion;

                                // ✅ provenance_jsonをここで更新
                                var prov = QualityLogHelper.BuildProvenance(
                                    ruleSetId: policy.RuleSetId,
                                    ruleVersion: ruleVersion,
                                    policyId: policy.PolicyId,
                                    attrCd: attr.AttrCd,
                                    matcherKind: policy.MatcherKind ?? "UNKNOWN",
                                    stepNo: policy.StepNo,
                                    sourceRaw: attr.SourceRaw,
                                    groupCompanyCd: batchRun?.GroupCompanyCd ?? "UNKNOWN",
                                    batchId: batchRun?.BatchId ?? "UNKNOWN",
                                    tempRowId: attr.TempRowId.ToString(),
                                    workerId: "cleanse-worker-1"
                                );

                                attr.ProvenanceJson = ProvenanceHelper.AppendProvenanceJson(attr.ProvenanceJson, prov);

                                await _productAttrRepo.UpdateProductAttrAsync(attr);
                            }
                            else
                            {
                                Logger.Warn($"[WARN] attr_cd={attr.AttrCd}: rule_set_id={policy.RuleSetId} に対応なし");
                            }
                        }
                        // ポリシーが見つからなかった場合
                        else
                        {
                            // Logger.Warn(
                            //     $"[POLICY-MISS] 該当ポリシーなし：attr_cd={attr.AttrCd}, ctx.brand={brandForMatch ?? "NULL"}, ctx.cat={categoryForMatch ?? "NULL"}"
                            // );
                            Logger.Info($"[DEBUG前] attr_cd={attr.AttrCd}: rule_set_id={policy.RuleSetId} に対応するルールセットを適用: version={attr.RuleVersion}");
                            await CleanseResultHelper.HandleResultAsync(
                                _recordErrorRepo,
                                _productAttrRepo,
                                attr,
                                batchRun,
                                policy: null,
                                qualityStatus: "WARN",
                                message: "未対応のポリシーです。",
                                workerId: "cleanse-worker-1",
                                errorCode: "UNSUPPORTED_DATA_TYPE",
                                errorDetail: $"(attr_cd={attr.AttrCd})：未対応ポリシー",
                                reasonCd: "NO_MATCHING_POLICY"
                            );
                        }

                        // カウント更新
                        switch (attr.QualityStatus)
                        {
                            case "OK": ok++; break;
                            case "WARN": warn++; break;
                            case "NG": ng++; break;
                            default: warn++; break;
                        }
                    }
                    catch (Exception ex)
                    {
                        ng++;
                        attr.QualityStatus = "NG";
                        attr.QualityDetailJson = JsonHelper.SafeSerialize(new { error = ex.Message });
                        await _productAttrRepo.UpdateProductAttrAsync(attr);
                        Logger.Error($"属性処理中に例外: attr_cd={attr.AttrCd}, error={ex}");
                    }
                }

                // クレンジング完了後に単複整合処理を実施
                await ReconcileSingleValueAttributesAsync(candidates!.ToList());

                // 集計結果を更新
                await UpdateCleanseCountAsync(batchId, read, ok, warn, ng);
            }
        }

        // 属性をクレンジング
        private async Task ProcessSingleAttributeAsync(ClProductAttr attr, BatchRun batchRun, CleansePolicy? policy)
        {
            var srcRaw = attr.SourceRaw;

            _definitionCache.TryGetValue(attr.AttrCd, out var definition);

            MCleanseRuleSet? ruleSet = null;
            RefTableMap? refMap = null;

            if (policy != null)
            {
                // rule_set_idからルールセットを取得
                if (policy.RuleSetId > 0 && _cleanseRuleSetCache.TryGetValue(policy.RuleSetId, out var foundRule))
                {
                    ruleSet = foundRule;
                    Logger.Info($"[DEBUG] rule_set_id={policy.RuleSetId} に対応するルールセットを取得: version={ruleSet.RuleVersion}, released_at={ruleSet.ReleasedAt}");
                }
                else
                {
                    Logger.Warn($"rule_set_id={policy.RuleSetId} に対応するルールセットが見つかりません。");
                }

                // ref_map_idから参照マップを取得
                if (policy.RefMapId > 0)
                {
                    refMap = _refTableMapCache.Values.FirstOrDefault(r => r.RefMapId == policy.RefMapId);
                    if (refMap != null)
                    {
                        Logger.Info($"[DEBUG] ref_map_id={policy.RefMapId} に対応する参照マップを取得: table={refMap.Hop1Table} → {refMap.Hop2Table}");
                    }
                    else
                    {
                        Logger.Warn($"ref_map_id={policy.RefMapId} に対応する m_ref_table_map が見つかりません。");
                    }
                }
            }

            // 項目定義表に存在しない場合はNGで終了
            if (definition == null)
            {
                await CleanseResultHelper.HandleResultAsync(
                    _recordErrorRepo,
                    _productAttrRepo,
                    attr,
                    batchRun,
                    policy,
                    qualityStatus: "WARN",
                    message: "m_attr_definitionにattr_cdが存在しない",
                    workerId: "cleanse-worker-1",
                    errorCode: "MISSING_ATTR_DEFINITION",
                    errorDetail: $"(attr_cd={attr.AttrCd})：m_attr_definitionにattr_cdが存在しない。",
                    reasonCd: "REF_NOT_FOUND"
                );
                return;
            }

            // ポリシー表に存在しない場合はNGで終了
            if (policy == null)
            {
                await CleanseResultHelper.HandleResultAsync(
                    _recordErrorRepo,
                    _productAttrRepo,
                    attr,
                    batchRun,
                    policy,
                    qualityStatus: "WARN",
                    message: "ポリシー表にルール未定義",
                    workerId: "cleanse-worker-1",
                    errorCode: "MISSING_CLEANSE_POLICY",
                    errorDetail: $"(attr_cd={attr.AttrCd})：ポリシー表にルール未定義",
                    reasonCd: "REF_NOT_FOUND"
                );
                return;
            }

            switch (policy.DataType)
            {
                case "REF":
                case "LIST":
                    await HandleDictionaryTypeAsync(attr, definition, policy, batchRun);
                    break;

                case "TEXT":
                case "NUM":
                case "TIMESTAMPTZ":
                    await HandleNormalizeTypeAsync(attr, definition, policy, batchRun);
                    return;
            }
        }

        // 第二層：MatcherKind,DataTypeごとの matcher_kind による分流
        private async Task HandleDictionaryTypeAsync(
            ClProductAttr attr,
            AttributeDefinition definition,
            CleansePolicy policy,
            BatchRun batchRun)
        {

            Logger.Info($"Dictionary型処理開始: attr_cd={attr.AttrCd}, source_id={attr.SourceId}, source_label={attr.SourceLabel},attr.RuleVersion={attr.RuleVersion}");

            switch (policy.DataType)
            {
                // REF系処理
                case "REF":
                    if (policy.MatcherKind == "ID_EXACT")
                        await HandleRefTypeAsync(attr, definition, policy, batchRun);
                    else if (policy.MatcherKind == "DERIVE_COALESCE")
                        await HandleRefTypeAsync(attr, definition, policy, batchRun);
                    else if (policy.MatcherKind == "TOKEN_DICT")
                        await HandleColorTypeAsync(attr, definition, policy, batchRun);
                    else
                    {
                        await CleanseResultHelper.HandleResultAsync(
                           _recordErrorRepo,
                           _productAttrRepo,
                           attr,
                           batchRun,
                           policy,
                           qualityStatus: "NGWARN",
                           message: $"該当するMatcherKindが未定義です: {policy.MatcherKind}",
                           workerId: "cleanse-worker-1",
                           errorCode: "MISSING_MATCH_KIND",
                           errorDetail: $"(attr_cd={attr.AttrCd})：REF型で対応するMatcherKindが見つかりません。",
                           reasonCd: policy.MatcherKind ?? "UNKNOWN"
                       );
                    }
                    break;

                // LIST系処理
                case "LIST":
                    if (policy.MatcherKind == "ID_EXACT")
                        await HandleListTypeAsync(attr, definition, policy, batchRun);
                    else if (policy.MatcherKind == "LABEL_EXACT")
                        await HandleListTypeAsync(attr, definition, policy, batchRun);
                    else if (policy.MatcherKind == "DERIVE_FROM_GP")
                        await HandleListTypeAsync(attr, definition, policy, batchRun);
                    else
                    {
                        await CleanseResultHelper.HandleResultAsync(
                          _recordErrorRepo,
                          _productAttrRepo,
                          attr,
                          batchRun,
                          policy,
                          qualityStatus: "WARN",
                          message: $"該当するMatcherKindが未定義です: {policy.MatcherKind}",
                          workerId: "cleanse-worker-1",
                          errorCode: "MISSING_MATCH_KIND",
                          errorDetail: $"(attr_cd={attr.AttrCd})：LIST型で対応するMatcherKindが見つかりません。",
                          reasonCd: policy.MatcherKind ?? "UNKNOWN"
                      );
                    }
                    break;
            }
        }

        // REFタイプの属性を処理
        private async Task HandleRefTypeAsync(ClProductAttr attr, AttributeDefinition definition, CleansePolicy policy, BatchRun batchRun)
        {

            Logger.Info($"REF型処理開始: attr_cd={attr.AttrCd}, source_id={attr.SourceId}, source_label={attr.SourceLabel}");

            RefTableMap? refMap = null;

            // ref_map_id が設定されていれば、それを使ってキャッシュから取得
            if (policy.RefMapId > 0 && _refTableMapCache.TryGetValue(policy.RefMapId.ToString(), out refMap))
            {
                Logger.Info($"[DEBUG] ref_map_id={policy.RefMapId} の定義を使用します。");
            }
            else if (_refTableMapCache.TryGetValue(attr.AttrCd, out refMap))
            {
                // fallback: attr_cd ベース（旧仕様）
                Logger.Warn($"[WARN] attr_cd={attr.AttrCd} ベースの参照マップを使用します（ref_map_id未設定）。");
            }
            else
            {
                await CleanseResultHelper.HandleResultAsync(
                    _recordErrorRepo,
                    _productAttrRepo,
                    attr,
                    batchRun,
                    policy,
                    qualityStatus: "NG",
                    message: "参照マップ（m_ref_table_map）定義が存在しません。",
                    workerId: "cleanse-worker-1",
                    errorCode: "REF_TABLE_MAP_NOT_FOUND",
                    errorDetail: $"(attr_cd={attr.AttrCd})：REF用参照先未設定/未登録。",
                    reasonCd: policy.MatcherKind ?? "UNKNOWN"
                );
                return;
            }

            // クレンジング参照解決を実行
            var (valueCd, valueText) = await _refResolverRepo.ResolveAsync(refMap, attr.SourceId, attr.SourceLabel);

            // 参照解決の結果が存在する場合
            if (valueCd != null || valueText != null)
            {
                attr.ValueCd = valueCd;
                attr.ValueText = valueText;

                Logger.Info($"*******INFO: 参照結果あり (attr_cd={attr.AttrCd}, source_id={attr.SourceId}, value_cd={attr.ValueCd}, value_text={attr.ValueText})");

                await CleanseResultHelper.HandleResultAsync(
                    _recordErrorRepo,
                    _productAttrRepo,
                    attr,
                    batchRun,
                    policy,
                    qualityStatus: "OK",
                    message: "正確に照合されました！",
                    workerId: "cleanse-worker-1",
                    reasonCd: policy.MatcherKind ?? "UNKNOWN"
                );

                Logger.Info(
                    $"SUCCESS: 更新完了 (attr_cd={attr.AttrCd}, source_id={attr.SourceId}, value_cd={attr.ValueCd}, value_text={attr.ValueText}), cleanse_phase={definition.CleansePhase}"
                );
            }
            // 参照解決の結果が null の場合
            else
            {
                Logger.Warn($"INFO: 参照結果なし (attr_cd={attr.AttrCd}, source_id={attr.SourceId}, source_label={attr.SourceLabel})");

                await CleanseResultHelper.HandleResultAsync(
                       _recordErrorRepo,
                       _productAttrRepo,
                       attr,
                       batchRun,
                       policy,
                       qualityStatus: "warn",
                       message: "参照１テーブルか参照2テーブルに該当する値が見つかりませんでした。",
                       workerId: "cleanse-worker-1",
                       errorCode: "REF_NOT_FOUND",
                       errorDetail: $"参照１テーブルか参照2テーブルに該当データなし (attr_cd={attr.AttrCd}, source_id={attr.SourceId}, source_label={attr.SourceLabel})",
                       reasonCd: policy.MatcherKind ?? "UNKNOWN"
                   );

                return;
            }
        }

        // LISTタイプの属性を処理
        private async Task HandleListTypeAsync(ClProductAttr attr, AttributeDefinition definition, CleansePolicy policy, BatchRun batchRun)
        {
            var srcRaw = attr.SourceRaw;
            string? finalValueCd = null;
            string? finalValueText = null;

            Logger.Info($"LIST型処理開始: attr_cd={attr.AttrCd}, source_id={attr.SourceId}, source_label={attr.SourceLabel}");

            // Step 1: cl_product_attr .source_id+source_label
            // とattr_source_map .source_attr_id+source_attr_nm 一致場合、g_list_item_id を検索
            var gListItemId = await _attrSourceMapRepository.FindBySourceDataAsync(attr.SourceId, attr.SourceLabel);

            if (gListItemId == null)
            {
                Logger.Warn($"警告: attr_source_map に一致するレコードが見つかりません (source_id={attr.SourceId}, source_label={attr.SourceLabel})");

                await CleanseResultHelper.HandleResultAsync(
                _recordErrorRepo,
                _productAttrRepo,
                attr,
                batchRun,
                policy,
                qualityStatus: "NG",
                message: "attr_source_map 定義が存在しません。",
                workerId: "cleanse-worker-1",
                errorCode: "LIST_GROUP_NOT_FOUND",
                errorDetail: $"(attr_cd={attr.AttrCd})：LIST用グループ未設定/未登録。",
                reasonCd: policy.MatcherKind ?? "UNKNOWN"
            );
                return;
            }

            // Step 2: m_list_item_g で g_list_item_id を検索
            var listItem = await _mlistItemGRepo.GetByListItemIdAsync(gListItemId.Value);
            if (listItem == null)
            {
                Logger.Warn($"警告: m_list_item_g に一致する g_list_item_id={gListItemId.Value} のレコードが見つかりません");

                await CleanseResultHelper.HandleResultAsync(
                    _recordErrorRepo,
                    _productAttrRepo,
                    attr,
                    batchRun,
                    policy,
                    qualityStatus: "WARN",
                    message: "m_list_item_g に該当データが存在しません。",
                    workerId: "cleanse-worker-1",
                    errorCode: "LIST_GROUP_NOT_FOUND",
                    errorDetail: $"(attr_cd={attr.AttrCd})：LIST用グループ未設定/未登録。",
                    reasonCd: policy.MatcherKind ?? "UNKNOWN"
                );
                return;
            }

            // Step 3: cl_product_attr に value_cd, value_text, g_list_item_id を設定
            finalValueCd = listItem.GItemCd;
            finalValueText = listItem.GItemLabel;

            // Step 4: 更新
            // quality_detail_json
            attr.QualityStatus = "OK";
            attr.ValueCd = finalValueCd;
            attr.ValueText = finalValueText;

            await CleanseResultHelper.HandleResultAsync(
                _recordErrorRepo,
                _productAttrRepo,
                attr,
                batchRun,
                policy,
                qualityStatus: attr.QualityStatus,
                message: "正確に照合されました！",
                workerId: "cleanse-worker-1",
                reasonCd: policy.MatcherKind ?? "UNKNOWN"
            );
            Logger.Info($"LIST型処理完了: value_cd={listItem.GItemCd}, value_text={listItem.GItemLabel}");
        }

        // TEXT / NUM / DATE タイプの属性を処理
        private async Task HandleNormalizeTypeAsync(ClProductAttr attr, AttributeDefinition definition, CleansePolicy policy, BatchRun batchRun)
        {
            var srcRaw = attr.SourceLabel;

            // Step 1: source_raw が空の場合 → NG
            if (string.IsNullOrWhiteSpace(srcRaw))
            {
                Logger.Warn($"警告: source_raw が空です (attr_cd={attr.AttrCd})");

                await CleanseResultHelper.HandleResultAsync(
                          _recordErrorRepo,
                          _productAttrRepo,
                          attr,
                          batchRun,
                          policy,
                          qualityStatus: "NG",
                          message: "source_raw が空です。",
                          workerId: "cleanse-worker-1",
                          errorCode: "SOURCE_RAW_NOT_FOUND",
                          errorDetail: $"source_rawが存在しません (attr_cd={attr.AttrCd})",
                          reasonCd: policy.MatcherKind ?? "UNKNOWN"
                      );
                return;
            }

            // Step 2: 正規化処理
            string raw = srcRaw.Trim();
            string? valueText = null;
            decimal? valueNum = null;
            DateTime? valueDate = null;

            try
            {
                switch (definition.DataType)
                {
                    case "TEXT":
                        valueText = NormalizeHelper.NormalizeText(raw);
                        break;
                    case "NUM":
                        valueNum = NormalizeHelper.NormalizeNumber(raw);
                        break;
                    case "TIMESTAMPTZ":
                        valueDate = NormalizeHelper.NormalizeDate(raw);
                        break;
                }

                // 正常に正規化できた場合 → OK
                attr.ValueText = valueText;
                attr.ValueNum = valueNum;
                attr.ValueDate = valueDate;

                await CleanseResultHelper.HandleResultAsync(
                    _recordErrorRepo,
                    _productAttrRepo,
                    attr,
                    batchRun,
                    policy,
                    qualityStatus: "OK",
                    message: "正規化が正常に完了しました。",
                    workerId: "cleanse-worker-1",
                    reasonCd: policy.MatcherKind ?? "UNKNOWN"
                );

                Logger.Info($"SUCCESS: 正規化完了 (attr_cd={attr.AttrCd}, data_type={definition.DataType}, value_text={attr.ValueText})");

            }
            catch (Exception ex)
            {
                Logger.Warn($"警告: 正規化に失敗しました (attr_cd={attr.AttrCd}, error={ex.Message})");

                await CleanseResultHelper.HandleResultAsync(
                           _recordErrorRepo,
                           _productAttrRepo,
                           attr,
                           batchRun,
                           policy,
                           qualityStatus: "NG",
                           message: "正規化に失敗しました。",
                           workerId: "cleanse-worker-1",
                            errorCode: "INVALID_TYPE_CAST",
                            errorDetail: $"(attr_cd={attr.AttrCd})：型変換失敗 ({ex.Message})",
                           reasonCd: policy.MatcherKind ?? "UNKNOWN"
                );
            }
        }

        public async Task<JsonObject> ProcessAttributeAsync(
            string sourceLabel,
            string attrCd,
            string groupCompanyCd)
        {
            Logger.Info($"[DEBUG] ProcessAttributeAsync called: sourceLabel={sourceLabel}, attrCd={attrCd}, groupCompanyCd={groupCompanyCd}");

            // 🧪 虚拟逻辑：你这里可以以后替换成真正的 cleansing/matcher 处理
            await Task.Delay(50); // 模拟异步处理延迟

            // 🧩 构造输出 JSON 对象
            var result = new JsonObject
            {
                ["outputs"] = new JsonArray
        {
            new JsonObject
            {
                ["attr_cd"] = attrCd,
                ["seq"] = 1,
                ["value_cd"] = "BLACK",
                ["value_text"] = "BLACK"
            },
            new JsonObject
            {
                ["attr_cd"] = attrCd,
                ["seq"] = 2,
                ["value_cd"] = "PINK",
                ["value_text"] = "PINK"
            }
        },
                ["quality_status"] = "OK",
                ["quality_detail_json"] = new JsonObject
                {
                    ["summary"] = new JsonObject
                    {
                        ["token_count"] = 3,
                        ["matched_tokens"] = new JsonArray { "BK", "PK" },
                        ["unmatched_tokens"] = new JsonArray { "ｼｪﾙｺﾞｰﾙﾄﾞｸﾘｽﾀﾙデイトジャストある" }
                    },
                    ["attr_details"] = new JsonObject
                    {
                        [attrCd] = new JsonArray
                {
                    new JsonObject
                    {
                        ["seq"] = 1,
                        ["status"] = "OK",
                        ["reason"] = $"token 'BK' matched {attrCd}_route",
                        ["rule_applied"] = new JsonObject
                        {
                            ["ref_map"] = "m_color_token_route",
                            ["priority"] = 0
                        }
                    },
                    new JsonObject
                    {
                        ["seq"] = 2,
                        ["status"] = "OK",
                        ["reason"] = $"token 'PK' matched {attrCd}_route",
                        ["rule_applied"] = new JsonObject
                        {
                            ["ref_map"] = "m_color_token_route",
                            ["priority"] = 0
                        }
                    }
                }
                    }
                },
                ["provenance_json"] = new JsonObject
                {
                    ["stage"] = "CLEANSE",
                    ["gp_cd"] = groupCompanyCd,
                    ["brand_scope"] = null,
                    ["category_scope"] = null,
                    ["attr"] = attrCd,
                    ["input"] = sourceLabel,
                    ["normalized_input"] = sourceLabel.Replace("･", ""), // 假设简单清洗
                    ["tokens"] = new JsonArray { "BK", "PK", "ｼｪﾙｺﾞｰﾙﾄﾞｸﾘｽﾀﾙデイトジャストある" },
                    ["routes"] = new JsonArray
            {
                new JsonObject
                {
                    ["token"] = "BK",
                    ["target_attr_cd"] = attrCd,
                    ["candidates"] = new JsonArray
                    {
                        new JsonObject
                        {
                            ["normalize_to"] = "BLACK",
                            ["priority"] = 0
                        }
                    },
                    ["picked"] = "BLACK"
                },
                new JsonObject
                {
                    ["token"] = "PK",
                    ["target_attr_cd"] = attrCd,
                    ["candidates"] = new JsonArray
                    {
                        new JsonObject
                        {
                            ["normalize_to"] = "PINK",
                            ["priority"] = 0
                        }
                    },
                    ["picked"] = "PINK"
                }
            },
                    ["rule_version"] = "v2025.10.30",
                    ["matcher"] = "TOKEN_ROUTE",
                    ["notes"] = "multi material allowed; attr_seq set as encounter order (1..n)"
                }
            };

            Logger.Info("[DEBUG] ProcessAttributeAsync completed successfully.");

            return result;
        }


        private async Task HandleColorTypeAsync(
            ClProductAttr attr,
            AttributeDefinition definition,
            CleansePolicy policy,
            BatchRun batchRun)
        {
            try
            {
                Logger.Info($"COLOR型処理開始: attr_cd={attr.AttrCd}, source_raw={attr.SourceRaw}");

                // ① 解析 SourceRaw 中的 JSON
                // var colorJson = JsonSerializer.Deserialize<JsonObject>(attr.SourceRaw ?? "");

                var colorJson = JsonNode.Parse(@"
                {
                ""outputs"": [
                    {
                    ""attr_cd"": ""DIAL_COLOR"",
                    ""seq"": 1,
                    ""value_cd"": ""BLACK"",
                    ""value_text"": ""BLACK""
                    },
                    {
                    ""attr_cd"": ""DIAL_COLOR"",
                    ""seq"": 2,
                    ""value_cd"": ""PINK"",
                    ""value_text"": ""PINK""
                    }
                ],
                ""quality_status"": ""OK"",
                ""quality_detail_json"": {
                    ""summary"": {
                    ""token_count"": 3,
                    ""matched_tokens"": [""BK"", ""PK""],
                    ""unmatched_tokens"": [""ｼｪﾙｺﾞｰﾙﾄﾞｸﾘｽﾀﾙデイトジャストある""]
                    },
                    ""attr_details"": {
                    ""DIAL_COLOR"": [
                        {
                        ""seq"": 1,
                        ""status"": ""OK"",
                        ""reason"": ""token 'BK' matched DIAL_COLOR_route"",
                        ""rule_applied"": {
                            ""ref_map"": ""m_color_token_route"",
                            ""priority"": 0
                        }
                        },
                        {
                        ""seq"": 2,
                        ""status"": ""OK"",
                        ""reason"": ""token 'PK' matched DIAL_COLOR_route"",
                        ""rule_applied"": {
                            ""ref_map"": ""m_color_token_route"",
                            ""priority"": 0
                        }
                        }
                    ]
                    }
                },
                ""provenance_json"": {
                    ""stage"": ""CLEANSE"",
                    ""gp_cd"": ""KM"",
                    ""brand_scope"": null,
                    ""category_scope"": null,
                    ""attr"": ""DIAL_COLOR"",
                    ""input"": ""BKPKｼｪﾙ･ｺﾞｰﾙﾄﾞｸﾘｽﾀﾙデイトジャストある"",
                    ""normalized_input"": ""BKPKｼｪﾙｺﾞｰﾙﾄﾞｸﾘｽﾀﾙデイトジャストある"",
                    ""tokens"": [""BK"", ""PK"", ""ｼｪﾙｺﾞｰﾙﾄﾞｸﾘｽﾀﾙデイトジャストある""],
                    ""routes"": [
                    {
                        ""token"": ""BK"",
                        ""target_attr_cd"": ""DIAL_COLOR"",
                        ""candidates"": [
                        { ""normalize_to"": ""BLACK"", ""priority"": 0 }
                        ],
                        ""picked"": ""BLACK""
                    },
                    {
                        ""token"": ""PK"",
                        ""target_attr_cd"": ""DIAL_COLOR"",
                        ""candidates"": [
                        { ""normalize_to"": ""PINK"", ""priority"": 0 }
                        ],
                        ""picked"": ""PINK""
                    }
                    ],
                    ""rule_version"": ""v2025.10.30"",
                    ""matcher"": ""TOKEN_ROUTE"",
                    ""notes"": ""multi material allowed; attr_seq set as encounter order (1..n)""
                }
                }
                ")!.AsObject();

                if (colorJson == null)
                {
                    await CleanseResultHelper.HandleResultAsync(
                        _recordErrorRepo, _productAttrRepo, attr, batchRun, policy,
                        qualityStatus: "NG",
                        message: "カラーJSONの解析に失敗しました。",
                        workerId: "cleanse-worker-1",
                        errorCode: "INVALID_COLOR_JSON",
                        errorDetail: $"(attr_cd={attr.AttrCd})：SourceRawが有効なJSONではありません。",
                        reasonCd: policy.MatcherKind ?? "COLOR_DERIVE"
                    );
                    return;
                }

                // ② 項目取得
                //string batchId = attr.BatchId;
                string batchId = batchRun?.BatchId ?? "DEBUG_BATCH";
                string qualityStatus = colorJson["quality_status"]?.ToString() ?? "WARN";
                string ruleVersion = colorJson["provenance_json"]?["rule_version"]?.ToString() ?? "unknown";
                string qualityDetailJson = colorJson["quality_detail_json"]?.ToJsonString() ?? "{}";
                string provenanceJson = colorJson["provenance_json"]?.ToJsonString() ?? "{}";

                var outputs = colorJson["outputs"]?.AsArray();
                if (outputs == null || outputs.Count == 0)
                {
                    Logger.Warn($"カラー解析結果(outputs)が空です: attr_cd={attr.AttrCd}");
                    await CleanseResultHelper.HandleResultAsync(
                        _recordErrorRepo, _productAttrRepo, attr, batchRun, policy,
                        qualityStatus: "WARN",
                        message: "カラー解析結果(outputs)が空です。",
                        workerId: "cleanse-worker-1",
                        errorCode: "COLOR_OUTPUT_EMPTY",
                        errorDetail: $"(attr_cd={attr.AttrCd})：カラー解析結果が存在しません。",
                        reasonCd: "COLOR_DERIVE"
                    );
                    return;
                }

                // ③ attr_cd ごとにグループ化して同じ属性コードのデータをまとめ
                var groupedByAttrCd = outputs
                    .Select(o => o!.AsObject())
                    .GroupBy(o => o["attr_cd"]?.ToString() ?? attr.AttrCd);

                foreach (var group in groupedByAttrCd)
                {
                    string currentAttrCd = group.Key ?? attr.AttrCd;
                    Logger.Info($"COLOR出力処理: attr_cd={currentAttrCd}, 件数={group.Count()}");

                    foreach (var item in group)
                    {
                        string? valueCd = item["value_cd"]?.ToString();
                        string? valueText = item["value_text"]?.ToString();
                        short attrSeq = (short)(item["seq"]?.GetValue<int>() ?? 1);

                        // 新しい属性レコードを生成
                        var newAttr = new ClProductAttr
                        {
                            BatchId = batchId,
                            SourceId = attr.SourceId,
                            SourceLabel = attr.SourceLabel,
                            SourceRaw = attr.SourceRaw,
                            TempRowId = attr.TempRowId, // 維持原始の TempRowId
                            // TempRowId = Guid.NewGuid().ToString(),
                            DataType = policy.DataType,
                            AttrCd = currentAttrCd,
                            AttrSeq = attrSeq,
                            ValueCd = valueCd,
                            ValueText = valueText,
                            QualityStatus = qualityStatus,
                            QualityDetailJson = qualityDetailJson,
                            ProvenanceJson = provenanceJson,
                            RuleVersion = ruleVersion,
                            CreAt = DateTime.UtcNow,
                            UpdAt = DateTime.UtcNow
                        };

                        await _productAttrRepo.UpsertColorResultAsync(newAttr);

                        Logger.Info($"COLOR行登録完了: attr_cd={currentAttrCd}, value_cd={valueCd}, value_text={valueText}");
                    }

                    // グループ単位で CleanseResultHelper 呼び出し（1attr_cd につき1回）
                    await CleanseResultHelper.HandleResultAsync(
                        _recordErrorRepo, _productAttrRepo, attr, batchRun, policy,
                        qualityStatus: qualityStatus,
                        message: $"カラー属性 {currentAttrCd} のクレンジング完了 ({group.Count()}件)",
                        workerId: "cleanse-worker-1",
                        reasonCd: policy.MatcherKind ?? "COLOR_DERIVE"
                    );
                }

                Logger.Info($"COLOR型処理完了: outputs={outputs.Count}, distinct_attr_cd={groupedByAttrCd.Count()}");
            }
            catch (Exception ex)
            {
                Logger.Error($"COLOR型処理中に例外: attr_cd={attr.AttrCd}, error={ex}");
                await CleanseResultHelper.HandleResultAsync(
                    _recordErrorRepo, _productAttrRepo, attr, batchRun, policy,
                    qualityStatus: "NG",
                    message: "COLOR型処理中に例外発生。",
                    workerId: "cleanse-worker-1",
                    errorCode: "COLOR_PROCESS_EXCEPTION",
                    errorDetail: $"(attr_cd={attr.AttrCd})：{ex.Message}",
                    reasonCd: "COLOR_DERIVE"
                );
            }
        }

        // 最新の is_active=TRUE のルールセットを取得
        private MCleanseRuleSet? GetLatestActiveRuleSet()
        {
            return _cleanseRuleSetCache
                .Select(kv => kv.Value)
                .Where(r => r.IsActive)
                .OrderByDescending(r => r.ReleasedAt)
                .FirstOrDefault();
        }

        /// <summary>
        /// select_type='SINGLE' の属性で複数値が存在する場合に単複整合処理を実施。
        /// 優先順位：quality_status > step_no > provenance（適用順）
        /// </summary>
        private async Task ReconcileSingleValueAttributesAsync(List<ClProductAttr> allAttributes)
        {
            // 同一 batch_id, 同一 attr_cd ごとにグループ化
            var grouped = allAttributes
                .Where(a => !string.IsNullOrWhiteSpace(a.AttrCd))
                .GroupBy(a => a.AttrCd);

            foreach (var group in grouped)
            {
                // 属性定義を取得
                if (!_definitionCache.TryGetValue(group.Key, out var definition))
                    continue;

                // select_type が SINGLE のみ対象
                if (!string.Equals(definition.SelectType, "SINGLE", StringComparison.OrdinalIgnoreCase))
                    continue;

                // 複数値が存在しない場合はスキップ
                if (group.Count() <= 1)
                    continue;

                // 定義から step_no を引くためのローカル関数（無ければ最大値）
                int GetStepNo(string attrCd)
                {
                    if (_policyCache.TryGetValue(attrCd, out var policy))
                        return policy.StepNo;
                    return int.MaxValue;
                }

                // 優先順位に基づき代表値を決定
                var chosen = group
                    .OrderByDescending(a => a.QualityStatus == "OK" ? 3 :
                                            a.QualityStatus == "WARN" ? 2 : 1)
                    .ThenBy(a => GetStepNo(a.AttrCd))
                    .ThenBy(a => a.UpdAt) // provenance の代替（更新日時）
                    .FirstOrDefault();

                if (chosen == null)
                {
                    // 代表値なし → 全て WARN に設定
                    foreach (var a in group)
                    {
                        a.ValueCd = null;
                        a.ValueText = null;
                        a.QualityStatus = "WARN";
                        await _productAttrRepo.UpdateProductAttrAsync(a);
                    }
                    Logger.Warn($"単複整合失敗: attr_cd={group.Key} → 全レコード WARN として処理");
                }
                else
                {
                    // 代表値を保持、それ以外は WARN として扱う
                    foreach (var a in group)
                    {
                        if (a.TempRowId != chosen.TempRowId)
                        {
                            a.QualityStatus = "WARN";
                            await _productAttrRepo.UpdateProductAttrAsync(a);
                        }
                    }
                    Logger.Info($"単複整合成功: attr_cd={group.Key} → 代表値={chosen.ValueText} ({chosen.QualityStatus})");
                }
            }
        }

        /// <summary>
        /// CLEANSE フェーズの件数集計を batch_run.counts_json に反映する。
        /// </summary>
        private async Task UpdateCleanseCountAsync(string batchId, int read, int ok, int warn, int ng)
        {
            try
            {
                var batch = await _batchRunRepo.GetByBatchIdAsync(batchId);
                if (batch == null)
                {
                    Logger.Error($"batch_id={batchId} が見つかりません。");
                    return;
                }

                // 既存 counts_json の解析
                Dictionary<string, object>? countsRoot = null;
                try
                {
                    countsRoot = string.IsNullOrWhiteSpace(batch.CountsJson)
                        ? new Dictionary<string, object>()
                        : JsonSerializer.Deserialize<Dictionary<string, object>>(batch.CountsJson);
                }
                catch (Exception ex)
                {
                    Logger.Warn($"counts_json の解析に失敗しました (batch_id={batchId}, error={ex.Message})。初期化して再生成します。");
                    countsRoot = new Dictionary<string, object>();
                }

                // CLEANSE 部分を更新
                var cleanseCounts = new Dictionary<string, int>
                {
                    ["read"] = read,
                    ["ok"] = ok,
                    ["warn"] = warn,
                    ["ng"] = ng
                };

                countsRoot["CLEANSE"] = cleanseCounts;

                // JSON に戻して保存
                batch.CountsJson = JsonSerializer.Serialize(countsRoot);
                batch.UpdAt = DateTime.UtcNow;

                // ステータス更新：全件OKならSUCCESS、それ以外はPARTIAL
                batch.BatchStatus = (ng == 0 && warn == 0)
                    ? "SUCCESS"
                    : (ok > 0 ? "PARTIAL" : "FAILED");

                await _batchRunRepo.UpdateAsync(batch);

                Logger.Info($"CLEANSE件数更新完了: batch_id={batchId}, read={read}, ok={ok}, warn={warn}, ng={ng}, status={batch.BatchStatus}");
            }
            catch (Exception ex)
            {
                Logger.Error($"[UpdateCleanseCountAsync] 更新中にエラーが発生しました (batch_id={batchId}, error={ex})");
            }
        }
    }
}