CREATE TABLE temp_product_event (
    temp_row_event_id UUID NOT NULL DEFAULT gen_random_uuid() PRIMARY KEY,
    batch_id TEXT NOT NULL,
    time_no BIGINT NOT NULL,
    idem_key TEXT NOT NULL,
    source_group_company_cd TEXT NOT NULL,
    source_product_id TEXT NOT NULL,
    source_store_id_raw TEXT,
    source_store_nm_raw TEXT,
    source_new_used_kbn_raw TEXT,
    event_ts_raw TEXT,
    event_kind_raw TEXT,
    qty_raw TEXT,
    extras_json JSONB,
    step_status TEXT NOT NULL DEFAULT 'READY',
    cre_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    upd_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (idem_key)
);

COMMENT ON TABLE temp_product_event IS '取込商品イベントテーブル';
COMMENT ON COLUMN temp_product_event.temp_row_event_id IS '取込イベント行ID';
COMMENT ON COLUMN temp_product_event.batch_id IS 'バッチID';
COMMENT ON COLUMN temp_product_event.time_no IS '行番号';
COMMENT ON COLUMN temp_product_event.idem_key IS '清掃キー';
COMMENT ON COLUMN temp_product_event.source_group_company_cd IS '連携元グループコード';
COMMENT ON COLUMN temp_product_event.source_product_id IS '連携元商品コード（注）';
COMMENT ON COLUMN temp_product_event.source_store_id_raw IS '連携元店舗コード（注）';
COMMENT ON COLUMN temp_product_event.source_store_nm_raw IS '連携元店舗名（注）';
COMMENT ON COLUMN temp_product_event.source_new_used_kbn_raw IS '新品/中古（注）';
COMMENT ON COLUMN temp_product_event.event_ts_raw IS '発生日時（注）';
COMMENT ON COLUMN temp_product_event.event_kind_raw IS 'イベント種別（注）';
COMMENT ON COLUMN temp_product_event.qty_raw IS '数量（注）';
COMMENT ON COLUMN temp_product_event.extras_json IS '付帯情報';
COMMENT ON COLUMN temp_product_event.step_status IS 'ステータス';
COMMENT ON COLUMN temp_product_event.cre_at IS '登録日時';
COMMENT ON COLUMN temp_product_event.upd_at IS '更新日時';

CREATE TABLE cl_product_event (
    id_event_id BIGINT NOT NULL PRIMARY KEY,
    temp_row_event_id UUID NOT NULL DEFAULT gen_random_uuid(),
    batch_id TEXT NOT NULL,
    idem_key TEXT NOT NULL UNIQUE,
    stock_effect_cd TEXT,
    signed_qty_num INT,
    reversal_idem_key TEXT,
    group_company_id BIGINT NOT NULL,
    g_product_id BIGINT NOT NULL,
    store_id BIGINT NOT NULL,
    new_used_kbn_cd TEXT,
    event_ts TIMESTAMPTZ NOT NULL,
    event_kind_cd TEXT NOT NULL,
    qty_num INT NOT NULL,
    quality_status TEXT,
    quality_detail_json JSONB,
    provenance_json JSONB,
    rule_version TEXT,
    cleanse_status TEXT NOT NULL DEFAULT 'READY',
    upsert_status TEXT,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    upd_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE cl_product_event IS 'クレンジングイベントテーブル';
COMMENT ON COLUMN cl_product_event.id_event_id IS 'クレンジングイベントID';
COMMENT ON COLUMN cl_product_event.temp_row_event_id IS 'Tempイベント行ID';
COMMENT ON COLUMN cl_product_event.batch_id IS 'バッチID';
COMMENT ON COLUMN cl_product_event.idem_key IS '標準キー';
COMMENT ON COLUMN cl_product_event.stock_effect_cd IS '在庫影響区分';
COMMENT ON COLUMN cl_product_event.signed_qty_num IS '在庫符号付数量';
COMMENT ON COLUMN cl_product_event.reversal_idem_key IS '取消対象標準キー';
COMMENT ON COLUMN cl_product_event.group_company_id IS 'GP会社ID';
COMMENT ON COLUMN cl_product_event.g_product_id IS 'G商品ID';
COMMENT ON COLUMN cl_product_event.store_id IS '店舗ID';
COMMENT ON COLUMN cl_product_event.new_used_kbn_cd IS '新品区分';
COMMENT ON COLUMN cl_product_event.event_ts IS '発生日時';
COMMENT ON COLUMN cl_product_event.event_kind_cd IS 'イベント種別コード';
COMMENT ON COLUMN cl_product_event.qty_num IS '数量';
COMMENT ON COLUMN cl_product_event.quality_status IS '品質判定フラグ';
COMMENT ON COLUMN cl_product_event.quality_detail_json IS '品質詳細';
COMMENT ON COLUMN cl_product_event.provenance_json IS '出処情報';
COMMENT ON COLUMN cl_product_event.rule_version IS '適用ルールバージョン';
COMMENT ON COLUMN cl_product_event.cleanse_status IS 'クレンジングステータス';
COMMENT ON COLUMN cl_product_event.upsert_status IS 'アップサートステータス';
COMMENT ON COLUMN cl_product_event.cre_at IS '登録日時';
COMMENT ON COLUMN cl_product_event.upd_at IS '更新日時';

CREATE TABLE m_product (
    g_product_id BIGINT NOT NULL PRIMARY KEY,
    g_product_cd TEXT NOT NULL,
    unit_no INT NOT NULL,
    group_company_id BIGINT NOT NULL,
    source_product_cd TEXT,
    source_product_management_cd TEXT,
    g_brand_id BIGINT,
    g_category_id BIGINT NOT NULL,
    currency_cd TEXT,
    display_price_incl_tax NUMERIC(12,2),
    product_status_cd TEXT NOT NULL DEFAULT 'PRODUCT_STATUS_UNKNOWN',
    new_used_kbn_cd TEXT NOT NULL DEFAULT 'PRODUCT_CONDITION_UNKNOWN',
    stock_existence_cd TEXT NOT NULL DEFAULT 'STOCK_UNKNOWN',
    sale_status_cd TEXT NOT NULL DEFAULT 'SALE_UNKNOWN',
    last_event_ts TIMESTAMPTZ,
    last_event_kind_cd TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL,
    upd_at TIMESTAMPTZ NOT NULL,
    UNIQUE (g_product_cd, unit_no)
);

COMMENT ON TABLE m_product IS '商品マスタ';
COMMENT ON COLUMN m_product.g_product_id IS '内部商品コード';
COMMENT ON COLUMN m_product.g_product_cd IS 'G商品コード';
COMMENT ON COLUMN m_product.unit_no IS 'G商品枝番';
COMMENT ON COLUMN m_product.group_company_id IS 'GP会社ID';
COMMENT ON COLUMN m_product.source_product_cd IS '連携元商品コード';
COMMENT ON COLUMN m_product.source_product_management_cd IS '連携元製品コード';
COMMENT ON COLUMN m_product.g_brand_id IS 'Gブランドコード';
COMMENT ON COLUMN m_product.g_category_id IS 'GカテゴリID（葉）';
COMMENT ON COLUMN m_product.currency_cd IS '通貨';
COMMENT ON COLUMN m_product.display_price_incl_tax IS '表示価格';
COMMENT ON COLUMN m_product.product_status_cd IS '商品状態';
COMMENT ON COLUMN m_product.new_used_kbn_cd IS '新品区分';
COMMENT ON COLUMN m_product.stock_existence_cd IS '在庫有無';
COMMENT ON COLUMN m_product.sale_status_cd IS '販売可否';
COMMENT ON COLUMN m_product.last_event_ts IS '最終イベント更新日時';
COMMENT ON COLUMN m_product.last_event_kind_cd IS '最終イベント種別コード';
COMMENT ON COLUMN m_product.is_active IS '有効フラグ';
COMMENT ON COLUMN m_product.cre_at IS '登録日時';
COMMENT ON COLUMN m_product.upd_at IS '更新日時';

CREATE TABLE m_token_route (
    token_id BIGINT NOT NULL PRIMARY KEY,
    group_company_cd TEXT NOT NULL,
    brand_scope BIGINT,
    category_scope BIGINT,
    token_label TEXT NOT NULL,
    token_label_norm TEXT,
    applicable_attr_cd TEXT,
    target_attr_cd TEXT,
    normalize_to TEXT,
    priority SMALLINT DEFAULT 10,
    token_remarks TEXT,
    is_active BOOLEAN DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    upd_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE m_token_route IS 'トークン分類辞書マスタ';
COMMENT ON COLUMN m_token_route.token_id IS 'トークンID';
COMMENT ON COLUMN m_token_route.group_company_cd IS 'GP会社コード';
COMMENT ON COLUMN m_token_route.brand_scope IS 'ブランド別トークン';
COMMENT ON COLUMN m_token_route.category_scope IS 'カテゴリ別トークン';
COMMENT ON COLUMN m_token_route.token_label IS 'トークン文字列';
COMMENT ON COLUMN m_token_route.token_label_norm IS 'トークン正規化文字列';
COMMENT ON COLUMN m_token_route.applicable_attr_cd IS 'トークン辞書適用項目';
COMMENT ON COLUMN m_token_route.target_attr_cd IS '書き込み先項目';
COMMENT ON COLUMN m_token_route.normalize_to IS '正規化名';
COMMENT ON COLUMN m_token_route.priority IS '優先度';
COMMENT ON COLUMN m_token_route.token_remarks IS '備考';
COMMENT ON COLUMN m_token_route.is_active IS '有効フラグ';
COMMENT ON COLUMN m_token_route.cre_at IS '登録日時';
COMMENT ON COLUMN m_token_route.upd_at IS '更新日時';

ALTER TABLE cl_product_event 
ADD CONSTRAINT fk_cl_event_temp_event 
FOREIGN KEY (temp_row_event_id) REFERENCES temp_product_event(temp_row_event_id);

ALTER TABLE cl_product_event 
ADD CONSTRAINT fk_cl_event_group_company 
FOREIGN KEY (group_company_id) REFERENCES m_company(group_company_id);

ALTER TABLE cl_product_event 
ADD CONSTRAINT fk_cl_event_product 
FOREIGN KEY (g_product_id) REFERENCES m_product(g_product_id);

ALTER TABLE cl_product_event 
ADD CONSTRAINT fk_cl_event_store 
FOREIGN KEY (store_id) REFERENCES m_store(store_id);

ALTER TABLE m_token_route 
ADD CONSTRAINT fk_token_route_brand 
FOREIGN KEY (brand_scope) REFERENCES m_brand_g(g_brand_id);

ALTER TABLE m_token_route 
ADD CONSTRAINT fk_token_route_category 
FOREIGN KEY (category_scope) REFERENCES m_category_g(g_category_id);

ALTER TABLE m_token_route 
ADD CONSTRAINT fk_token_route_attr 
FOREIGN KEY (target_attr_cd) REFERENCES m_attr_definition(attr_cd);