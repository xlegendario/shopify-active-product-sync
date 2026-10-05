-- Same as upsert_store_listings_keep_sku, but a row whose content did not
-- change is not written at all. store_listings carries nine indexes and the
-- old function rewrote every row it was handed, so each webhook and every
-- nightly pass rewrote whole catalogues for nothing - which is what took the
-- database down on 05-10-2026. Timestamps are left out of the comparison:
-- they only move when something real moved. Returns the rows written.
create or replace function public.upsert_store_listings_changed(rows jsonb)
returns integer
language plpgsql
as $function$
declare
  written integer;
begin
  insert into store_listings (
    merchant_record_id, merchant_name, shopify_product_id, shopify_variant_id,
    shopify_inventory_item_id, shopify_product_name, size, sku, shopify_sku,
    stockx_product_name, brand, picture_url, retailed_status, match_risk_level,
    status, last_seen_sync_id, last_shopify_sync_at, updated_at
  )
  select
    r->>'merchant_record_id', r->>'merchant_name', r->>'shopify_product_id', r->>'shopify_variant_id',
    r->>'shopify_inventory_item_id', r->>'shopify_product_name', r->>'size', r->>'sku', r->>'shopify_sku',
    r->>'stockx_product_name', r->>'brand', r->>'picture_url', r->>'retailed_status', r->>'match_risk_level',
    r->>'status', r->>'last_seen_sync_id', (r->>'last_shopify_sync_at')::timestamptz, (r->>'updated_at')::timestamptz
  from jsonb_array_elements(rows) r
  on conflict (merchant_record_id, shopify_product_id, shopify_variant_id)
  do update set
    merchant_name = excluded.merchant_name,
    shopify_inventory_item_id = excluded.shopify_inventory_item_id,
    shopify_product_name = excluded.shopify_product_name,
    size = excluded.size,
    -- Same rule as upsert_store_listings_keep_sku: a hand correction stands
    -- until the store changes its own SKU.
    sku = case
            when store_listings.shopify_sku is null then store_listings.sku
            when store_listings.shopify_sku is distinct from excluded.shopify_sku then excluded.sku
            else store_listings.sku
          end,
    shopify_sku = excluded.shopify_sku,
    stockx_product_name = excluded.stockx_product_name,
    brand = excluded.brand,
    picture_url = excluded.picture_url,
    retailed_status = excluded.retailed_status,
    match_risk_level = excluded.match_risk_level,
    status = excluded.status,
    last_seen_sync_id = excluded.last_seen_sync_id,
    last_shopify_sync_at = excluded.last_shopify_sync_at,
    updated_at = excluded.updated_at
  where
    (store_listings.merchant_name, store_listings.shopify_inventory_item_id, store_listings.shopify_product_name,
     store_listings.size, store_listings.shopify_sku, store_listings.stockx_product_name, store_listings.brand,
     store_listings.picture_url, store_listings.retailed_status, store_listings.match_risk_level, store_listings.status)
    is distinct from
    (excluded.merchant_name, excluded.shopify_inventory_item_id, excluded.shopify_product_name,
     excluded.size, excluded.shopify_sku, excluded.stockx_product_name, excluded.brand,
     excluded.picture_url, excluded.retailed_status, excluded.match_risk_level, excluded.status);

  get diagnostics written = row_count;
  return written;
end;
$function$;
