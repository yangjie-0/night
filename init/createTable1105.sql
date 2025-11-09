-- =====================================================
-- 商品マスタ (m_product)
-- =====================================================
-- CREATE TABLE m_product (
--     g_product_id BIGINT NOT NULL PRIMARY KEY,
--     g_product_cd TEXT NOT NULL,
--     unit_no INT NOT NULL,
--     group_company_id BIGINT NOT NULL,
--     source_product_cd TEXT,
--     source_product_management_cd TEXT,
--     g_brand_id BIGINT,
--     g_category_id BIGINT NOT NULL,
--     currency_cd TEXT,
--     display_price_incl_tax NUMERIC(12,2),
--     product_status_cd TEXT NOT NULL DEFAULT 'PRODUCT_STATUS_UNKNOWN',
--     new_used_kbn_cd TEXT NOT NULL DEFAULT 'PRODUCT_CONDITION_UNKNOWN',
--     stock_existence_cd TEXT NOT NULL DEFAULT 'STOCK_UNKNOWN',
--     sale_status_cd TEXT NOT NULL DEFAULT 'SALE_UNKNOWN',
--     last_event_ts TIMESTAMPTZ,
--     last_event_kind_cd TEXT,
--     is_active BOOLEAN NOT NULL DEFAULT TRUE,
--     cre_at TIMESTAMPTZ NOT NULL,
--     upd_at TIMESTAMPTZ NOT NULL,
--     UNIQUE (g_product_cd, unit_no)
-- );

-- COMMENT ON TABLE m_product IS '商品マスタ';
-- COMMENT ON COLUMN m_product.g_product_id IS '内部商品コード';
-- COMMENT ON COLUMN m_product.g_product_cd IS 'G商品コード';
-- COMMENT ON COLUMN m_product.unit_no IS 'G商品枝番';
-- COMMENT ON COLUMN m_product.group_company_id IS 'GP会社ID';
-- COMMENT ON COLUMN m_product.source_product_cd IS '連携元商品コード';
-- COMMENT ON COLUMN m_product.source_product_management_cd IS '連携元製品コード';
-- COMMENT ON COLUMN m_product.g_brand_id IS 'Gブランドコード';
-- COMMENT ON COLUMN m_product.g_category_id IS 'GカテゴリID（葉）';
-- COMMENT ON COLUMN m_product.currency_cd IS '通貨';
-- COMMENT ON COLUMN m_product.display_price_incl_tax IS '表示価格';
-- COMMENT ON COLUMN m_product.product_status_cd IS '商品状態';
-- COMMENT ON COLUMN m_product.new_used_kbn_cd IS '新品区分';
-- COMMENT ON COLUMN m_product.stock_existence_cd IS '在庫有無';
-- COMMENT ON COLUMN m_product.sale_status_cd IS '販売可否';
-- COMMENT ON COLUMN m_product.last_event_ts IS '最終イベント更新日時';
-- COMMENT ON COLUMN m_product.last_event_kind_cd IS '最終イベント種別コード';
-- COMMENT ON COLUMN m_product.is_active IS '有効フラグ';
-- COMMENT ON COLUMN m_product.cre_at IS '登録日時';
-- COMMENT ON COLUMN m_product.upd_at IS '更新日時';

-- =====================================================
-- 商品EAVマスタ (m_product_eav)
-- =====================================================
CREATE TABLE m_product_eav (
    g_product_id BIGINT NOT NULL,
    attr_cd TEXT NOT NULL,
    attr_seq SMALLINT NOT NULL DEFAULT 1,
    value_text TEXT,
    value_num NUMERIC(12,2),
    value_date TIMESTAMPTZ,
    value_cd TEXT,
    unit_cd TEXT,
    quality_status TEXT,
    quality_detail_json JSONB,
    provenance_json JSONB,
    batch_id TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL,
    upd_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (g_product_id, attr_cd, attr_seq)
);

COMMENT ON TABLE m_product_eav IS '商品EAVマスタ';
COMMENT ON COLUMN m_product_eav.g_product_id IS '内部商品コード';
COMMENT ON COLUMN m_product_eav.attr_cd IS '項目コード';
COMMENT ON COLUMN m_product_eav.attr_seq IS '順序';
COMMENT ON COLUMN m_product_eav.value_text IS '属性値（文字列）';
COMMENT ON COLUMN m_product_eav.value_num IS '属性値（数値）';
COMMENT ON COLUMN m_product_eav.value_date IS '属性値（日付）';
COMMENT ON COLUMN m_product_eav.value_cd IS '属性値（コード値）';
COMMENT ON COLUMN m_product_eav.unit_cd IS '単位コード';
COMMENT ON COLUMN m_product_eav.quality_status IS 'クレンジング品質フラグ';
COMMENT ON COLUMN m_product_eav.quality_detail_json IS 'クレンジング詳細情報';
COMMENT ON COLUMN m_product_eav.provenance_json IS '属性由来情報';
COMMENT ON COLUMN m_product_eav.batch_id IS 'バッチID';
COMMENT ON COLUMN m_product_eav.is_active IS '有効フラグ';
COMMENT ON COLUMN m_product_eav.cre_at IS '登録日時';
COMMENT ON COLUMN m_product_eav.upd_at IS '更新日時';

-- =====================================================
-- 商品運用状態マスタ (m_product_oper_state)
-- =====================================================
CREATE TABLE m_product_oper_state (
    g_product_id BIGINT NOT NULL PRIMARY KEY,
    transfer_status_cd TEXT NOT NULL DEFAULT 'TRANSFER_STATUS_UNKNOWN',
    repair_status_cd TEXT NOT NULL DEFAULT 'REPAIR_STATUS_UNKNOWN',
    reservation_status_cd TEXT NOT NULL DEFAULT 'RESERVATION_STATUS_UNKNOWN',
    consignment_status_cd TEXT NOT NULL DEFAULT 'CONSIGNMENT_STATUS_UNKNOWN',
    accept_status_cd TEXT NOT NULL DEFAULT 'ACCEPT_STATUS_UNKNOWN',
    current_store_id BIGINT,
    consignor_group_company_id BIGINT,
    consignor_product_id BIGINT,
    ec_listing_status_cd TEXT NOT NULL DEFAULT 'EC_LISTING_UNKNOWN',
    last_event_ts TIMESTAMPTZ,
    last_event_kind_cd TEXT,
    state_version TEXT,
    cre_at TIMESTAMPTZ NOT NULL,
    upd_at TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE m_product_oper_state IS '商品運用状態マスタ';
COMMENT ON COLUMN m_product_oper_state.g_product_id IS '内部商品コード';
COMMENT ON COLUMN m_product_oper_state.transfer_status_cd IS '移動状態';
COMMENT ON COLUMN m_product_oper_state.repair_status_cd IS '修理状態';
COMMENT ON COLUMN m_product_oper_state.reservation_status_cd IS '予約状態';
COMMENT ON COLUMN m_product_oper_state.consignment_status_cd IS '委託状態';
COMMENT ON COLUMN m_product_oper_state.accept_status_cd IS '受託状態';
COMMENT ON COLUMN m_product_oper_state.current_store_id IS '在庫店舗';
COMMENT ON COLUMN m_product_oper_state.consignor_group_company_id IS '委託元GP会社ID';
COMMENT ON COLUMN m_product_oper_state.consignor_product_id IS '委託元商品コード';
COMMENT ON COLUMN m_product_oper_state.ec_listing_status_cd IS 'EC掲載区分';
COMMENT ON COLUMN m_product_oper_state.last_event_ts IS '最終イベント更新日時';
COMMENT ON COLUMN m_product_oper_state.last_event_kind_cd IS '最終イベント種別コード';
COMMENT ON COLUMN m_product_oper_state.state_version IS '状態バージョン';
COMMENT ON COLUMN m_product_oper_state.cre_at IS '登録日時';
COMMENT ON COLUMN m_product_oper_state.upd_at IS '更新日時';

-- =====================================================
-- 商品画像マスタ (m_product_image)
-- =====================================================
CREATE TABLE m_product_image (
    g_product_id BIGINT NOT NULL,
    image_seq SMALLINT NOT NULL,
    image_s3_key TEXT NOT NULL,
    image_etag TEXT,
    image_type_cd TEXT,
    thumbnail_url TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL,
    upd_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (g_product_id, image_seq)
);

COMMENT ON TABLE m_product_image IS '商品画像マスタ';
COMMENT ON COLUMN m_product_image.g_product_id IS '商品ID';
COMMENT ON COLUMN m_product_image.image_seq IS '画像連番';
COMMENT ON COLUMN m_product_image.image_s3_key IS 'S3キー';
COMMENT ON COLUMN m_product_image.image_etag IS 'ETag';
COMMENT ON COLUMN m_product_image.image_type_cd IS '画像種別';
COMMENT ON COLUMN m_product_image.thumbnail_url IS '画像URL';
COMMENT ON COLUMN m_product_image.is_active IS '有効フラグ';
COMMENT ON COLUMN m_product_image.cre_at IS '登録日時';
COMMENT ON COLUMN m_product_image.upd_at IS '更新日時';

-- =====================================================
-- 商品同定マップ (m_product_ident)
-- =====================================================
CREATE TABLE m_product_ident (
    ident_id BIGINT NOT NULL PRIMARY KEY,
    g_product_id BIGINT NOT NULL,
    group_company_id BIGINT NOT NULL,
    source_product_cd TEXT,
    source_product_management_cd TEXT,
    ident_kind TEXT DEFAULT 'AUTO',
    confidence NUMERIC(5,2),
    is_primary BOOLEAN DEFAULT TRUE,
    is_active BOOLEAN,
    valid_from TIMESTAMPTZ DEFAULT NOW(),
    valid_to TIMESTAMPTZ,
    provenance_json JSONB DEFAULT '{}',
    ident_remarks TEXT,
    batch_id TEXT,
    cre_at TIMESTAMPTZ NOT NULL,
    upd_at TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE m_product_ident IS '商品同定マップ';
COMMENT ON COLUMN m_product_ident.ident_id IS '同定ID';
COMMENT ON COLUMN m_product_ident.g_product_id IS '内部商品コード';
COMMENT ON COLUMN m_product_ident.group_company_id IS 'GP会社ID';
COMMENT ON COLUMN m_product_ident.source_product_cd IS '連携元商品コード';
COMMENT ON COLUMN m_product_ident.source_product_management_cd IS '連携元製品コード';
COMMENT ON COLUMN m_product_ident.ident_kind IS '同定方法';
COMMENT ON COLUMN m_product_ident.confidence IS '信頼度';
COMMENT ON COLUMN m_product_ident.is_primary IS '同一source内での代表紐付けか';
COMMENT ON COLUMN m_product_ident.is_active IS '現行有効フラグ（履歴切替用）';
COMMENT ON COLUMN m_product_ident.valid_from IS '有効開始';
COMMENT ON COLUMN m_product_ident.valid_to IS '有効終了（NULL=現役）';
COMMENT ON COLUMN m_product_ident.provenance_json IS '由来';
COMMENT ON COLUMN m_product_ident.ident_remarks IS '備考';
COMMENT ON COLUMN m_product_ident.batch_id IS 'バッチID';
COMMENT ON COLUMN m_product_ident.cre_at IS '登録日時';
COMMENT ON COLUMN m_product_ident.upd_at IS '更新日時';

-- =====================================================
-- 一意制約（部分ユニーク） yang add
-- =====================================================
CREATE UNIQUE INDEX IF NOT EXISTS idx_m_product_ident_active_unique
    ON m_product_ident (group_company_id, source_product_cd)
    WHERE is_active = TRUE;

-- =====================================================
-- 検索用補助インデックス   yang add
-- =====================================================
CREATE INDEX idx_m_product_ident_product
    ON m_product_ident (g_product_id);

CREATE INDEX idx_m_product_ident_source_cd
    ON m_product_ident (source_product_cd);

-- =====================================================
-- 🔧 商品ID・同定ID 用のシーケンス作成 (yang add)
-- =====================================================

-- 商品ID 用シーケンス
CREATE SEQUENCE IF NOT EXISTS m_product_g_product_id_seq START 1 OWNED BY m_product.g_product_id;

ALTER TABLE m_product
  ALTER COLUMN g_product_id SET DEFAULT nextval('m_product_g_product_id_seq');

-- 同定ID 用シーケンス
CREATE SEQUENCE IF NOT EXISTS m_product_ident_ident_id_seq START 1 OWNED BY m_product_ident.ident_id;

ALTER TABLE m_product_ident
  ALTER COLUMN ident_id SET DEFAULT nextval('m_product_ident_ident_id_seq');

-- =====================================================
-- 製品マスタ (m_product_management)
-- =====================================================
CREATE TABLE m_product_management (
    g_product_management_id BIGINT NOT NULL PRIMARY KEY,
    group_company_id BIGINT NOT NULL,
    source_product_management_cd TEXT NOT NULL,
    g_brand_id BIGINT,
    g_category_id BIGINT NOT NULL,
    description_text TEXT,
    is_provisional BOOLEAN,
    source_product_cd BIGINT,
    provenance_json JSONB DEFAULT '{}',
    batch_id TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL,
    upd_at TIMESTAMPTZ NOT NULL
);

COMMENT ON TABLE m_product_management IS '製品マスタ';
COMMENT ON COLUMN m_product_management.g_product_management_id IS '製品ID';
COMMENT ON COLUMN m_product_management.group_company_id IS 'GP会社ID';
COMMENT ON COLUMN m_product_management.source_product_management_cd IS '連携元製品コード（KM製品コード）';
COMMENT ON COLUMN m_product_management.g_brand_id IS 'Gブランドコード';
COMMENT ON COLUMN m_product_management.g_category_id IS 'GカテゴリID（葉）';
COMMENT ON COLUMN m_product_management.description_text IS '製品説明（要約・代表）';
COMMENT ON COLUMN m_product_management.is_provisional IS '仮製品フラグ';
COMMENT ON COLUMN m_product_management.source_product_cd IS '仮製品元商品';
COMMENT ON COLUMN m_product_management.provenance_json IS '由来';
COMMENT ON COLUMN m_product_management.batch_id IS '最終更新に関わったバッチ';
COMMENT ON COLUMN m_product_management.is_active IS '有効フラグ';
COMMENT ON COLUMN m_product_management.cre_at IS '登録日時';
COMMENT ON COLUMN m_product_management.upd_at IS '更新日時';

-- =====================================================
-- 製品EAVマスタ (m_product_management_eav)
-- =====================================================
CREATE TABLE m_product_management_eav (
    g_product_management_id BIGINT NOT NULL,
    attr_cd TEXT NOT NULL,
    attr_seq SMALLINT NOT NULL DEFAULT 1,
    value_text TEXT,
    value_num NUMERIC(12,2),
    value_date DATE,
    value_cd TEXT,
    unit_cd TEXT,
    quality_status TEXT,
    quality_detail_json JSONB,
    provenance_json JSONB,
    batch_id TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL,
    upd_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (g_product_management_id, attr_cd, attr_seq)
);

COMMENT ON TABLE m_product_management_eav IS '製品EAVマスタ';
COMMENT ON COLUMN m_product_management_eav.g_product_management_id IS '製品ID';
COMMENT ON COLUMN m_product_management_eav.attr_cd IS '項目コード';
COMMENT ON COLUMN m_product_management_eav.attr_seq IS '順序';
COMMENT ON COLUMN m_product_management_eav.value_text IS '属性値（文字列）';
COMMENT ON COLUMN m_product_management_eav.value_num IS '属性値（数値）';
COMMENT ON COLUMN m_product_management_eav.value_date IS '属性値（日付）';
COMMENT ON COLUMN m_product_management_eav.value_cd IS '属性値（コード値）';
COMMENT ON COLUMN m_product_management_eav.unit_cd IS '単位コード';
COMMENT ON COLUMN m_product_management_eav.quality_status IS 'クレンジング品質フラグ';
COMMENT ON COLUMN m_product_management_eav.quality_detail_json IS 'クレンジング詳細情報';
COMMENT ON COLUMN m_product_management_eav.provenance_json IS '属性由来情報';
COMMENT ON COLUMN m_product_management_eav.batch_id IS 'バッチID';
COMMENT ON COLUMN m_product_management_eav.is_active IS '有効フラグ';
COMMENT ON COLUMN m_product_management_eav.cre_at IS '登録日時';
COMMENT ON COLUMN m_product_management_eav.upd_at IS '更新日時';

-- =====================================================
-- 製品画像マスタ (m_product_management_image)
-- =====================================================
CREATE TABLE m_product_management_image (
    g_product_management_id BIGINT NOT NULL,
    image_seq SMALLINT NOT NULL,
    image_s3_key TEXT NOT NULL,
    image_etag TEXT,
    image_type_cd TEXT,
    is_primary BOOLEAN,
    thumbnail_url TEXT,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    cre_at TIMESTAMPTZ NOT NULL,
    upd_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (g_product_management_id, image_seq)
);

COMMENT ON TABLE m_product_management_image IS '製品画像マスタ';
COMMENT ON COLUMN m_product_management_image.g_product_management_id IS '商品ID';
COMMENT ON COLUMN m_product_management_image.image_seq IS '画像連番';
COMMENT ON COLUMN m_product_management_image.image_s3_key IS 'S3キー';
COMMENT ON COLUMN m_product_management_image.image_etag IS 'ETag';
COMMENT ON COLUMN m_product_management_image.image_type_cd IS '画像種別';
COMMENT ON COLUMN m_product_management_image.is_primary IS '代表画像フラグ';
COMMENT ON COLUMN m_product_management_image.thumbnail_url IS '画像URL';
COMMENT ON COLUMN m_product_management_image.is_active IS '有効フラグ';
COMMENT ON COLUMN m_product_management_image.cre_at IS '登録日時';
COMMENT ON COLUMN m_product_management_image.upd_at IS '更新日時';

-- =====================================================
-- 外键约束
-- =====================================================

-- m_product 外键约束
ALTER TABLE m_product ADD CONSTRAINT fk_product_group_company FOREIGN KEY (group_company_id) REFERENCES m_company(group_company_id);
ALTER TABLE m_product ADD CONSTRAINT fk_product_brand FOREIGN KEY (g_brand_id) REFERENCES m_brand_g(g_brand_id);
ALTER TABLE m_product ADD CONSTRAINT fk_product_category FOREIGN KEY (g_category_id) REFERENCES m_category_g(g_category_id);

-- m_product_eav 外键约束
ALTER TABLE m_product_eav ADD CONSTRAINT fk_product_eav_product FOREIGN KEY (g_product_id) REFERENCES m_product(g_product_id);
ALTER TABLE m_product_eav ADD CONSTRAINT fk_product_eav_attr FOREIGN KEY (attr_cd) REFERENCES m_attr_definition(attr_cd);

-- m_product_oper_state 外键约束
ALTER TABLE m_product_oper_state ADD CONSTRAINT fk_oper_state_product FOREIGN KEY (g_product_id) REFERENCES m_product(g_product_id);
ALTER TABLE m_product_oper_state ADD CONSTRAINT fk_oper_state_store FOREIGN KEY (current_store_id) REFERENCES m_store(store_id);
ALTER TABLE m_product_oper_state ADD CONSTRAINT fk_oper_state_consignor_company FOREIGN KEY (consignor_group_company_id) REFERENCES m_company(group_company_id);
ALTER TABLE m_product_oper_state ADD CONSTRAINT fk_oper_state_consignor_product FOREIGN KEY (consignor_product_id) REFERENCES m_product(g_product_id);

-- m_product_image 外键约束
ALTER TABLE m_product_image ADD CONSTRAINT fk_product_image_product FOREIGN KEY (g_product_id) REFERENCES m_product(g_product_id);

-- m_product_ident 外键约束----yang add
ALTER TABLE m_product_ident
    ADD CONSTRAINT fk_product_ident_product
        FOREIGN KEY (g_product_id)
        REFERENCES m_product (g_product_id)
        DEFERRABLE INITIALLY DEFERRED;

ALTER TABLE m_product_ident
    ADD CONSTRAINT fk_product_ident_company
        FOREIGN KEY (group_company_id)
        REFERENCES m_company (group_company_id);

-- m_product_management 外键约束
ALTER TABLE m_product_management ADD CONSTRAINT fk_product_management_brand FOREIGN KEY (g_brand_id) REFERENCES m_brand_g(g_brand_id);
ALTER TABLE m_product_management ADD CONSTRAINT fk_product_management_category FOREIGN KEY (g_category_id) REFERENCES m_category_g(g_category_id);

-- m_product_management_eav 外键约束
ALTER TABLE m_product_management_eav ADD CONSTRAINT fk_pm_eav_product_management FOREIGN KEY (g_product_management_id) REFERENCES m_product_management(g_product_management_id);
ALTER TABLE m_product_management_eav ADD CONSTRAINT fk_pm_eav_attr FOREIGN KEY (attr_cd) REFERENCES m_attr_definition(attr_cd);

-- m_product_management_image 外键约束
ALTER TABLE m_product_management_image ADD CONSTRAINT fk_pm_image_product_management FOREIGN KEY (g_product_management_id) REFERENCES m_product_management(g_product_management_id);
