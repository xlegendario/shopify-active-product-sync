-- The store's own current price per variant, for My Shelf.
--
-- store_price is what the shop itself asks right now, whoever set it.
-- shopify_price (already there) stays what WE last pushed. Deliberately no
-- index on store_price: a repricing app can change prices hundreds of times
-- a minute, and an unindexed column lets Postgres update the row in place.

alter table public.store_listings add column if not exists store_price numeric;
alter table public.store_listings add column if not exists store_price_at timestamptz;

-- The full upsert, now carrying store_price as well. A row is still only
-- written when something in it really changed.
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
    status, last_seen_sync_id, last_shopify_sync_at, updated_at,
    store_price, store_price_at
  )
  select
    r->>'merchant_record_id', r->>'merchant_name', r->>'shopify_product_id', r->>'shopify_variant_id',
    r->>'shopify_inventory_item_id', r->>'shopify_product_name', r->>'size', r->>'sku', r->>'shopify_sku',
    r->>'stockx_product_name', r->>'brand', r->>'picture_url', r->>'retailed_status', r->>'match_risk_level',
    r->>'status', r->>'last_seen_sync_id', (r->>'last_shopify_sync_at')::timestamptz, (r->>'updated_at')::timestamptz,
    (r->>'store_price')::numeric, case when r ? 'store_price' then now() end
  from jsonb_array_elements(rows) r
  on conflict (merchant_record_id, shopify_product_id, shopify_variant_id)
  do update set
    merchant_name = excluded.merchant_name,
    shopify_inventory_item_id = excluded.shopify_inventory_item_id,
    shopify_product_name = excluded.shopify_product_name,
    size = excluded.size,
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
    updated_at = excluded.updated_at,
    -- A row without a price in it keeps the one it had.
    store_price = coalesce(excluded.store_price, store_listings.store_price),
    store_price_at = case
                       when excluded.store_price is not null
                        and excluded.store_price is distinct from store_listings.store_price
                       then now()
                       else store_listings.store_price_at
                     end
  where
    (store_listings.merchant_name, store_listings.shopify_inventory_item_id, store_listings.shopify_product_name,
     store_listings.size, store_listings.shopify_sku, store_listings.stockx_product_name, store_listings.brand,
     store_listings.picture_url, store_listings.retailed_status, store_listings.match_risk_level, store_listings.status)
    is distinct from
    (excluded.merchant_name, excluded.shopify_inventory_item_id, excluded.shopify_product_name,
     excluded.size, excluded.shopify_sku, excluded.stockx_product_name, excluded.brand,
     excluded.picture_url, excluded.retailed_status, excluded.match_risk_level, excluded.status)
    or (excluded.store_price is not null and excluded.store_price is distinct from store_listings.store_price);

  get diagnostics written = row_count;
  return written;
end;
$function$;

-- Prices only, straight from a webhook: no product read, no SKU lookup.
-- Rows: [{ merchant_record_id, product_id, variant_id, price }]. Reached
-- through the (merchant, product, variant) index; only real changes write.
create or replace function public.set_store_prices(rows jsonb)
returns integer
language plpgsql
as $function$
declare
  written integer;
begin
  update store_listings sl
  set store_price = (r->>'price')::numeric,
      store_price_at = now()
  from jsonb_array_elements(rows) r
  where sl.merchant_record_id = r->>'merchant_record_id'
    and sl.shopify_product_id = r->>'product_id'
    and sl.shopify_variant_id = r->>'variant_id'
    and sl.store_price is distinct from (r->>'price')::numeric;

  get diagnostics written = row_count;
  return written;
end;
$function$;
