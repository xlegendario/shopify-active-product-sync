-- The store catalogue for My Shelf: one row per product per store, the
-- choices a store makes about our consignment stock, and when each SKU first
-- appeared in consignment. Adds tables and functions only; nothing existing
-- is changed or removed.

-- 1. One row per product per store -------------------------------------------
--
-- store_listings is one row per variant: 1.2 million rows, and grouping ALC's
-- 348,000 of them per page view took 108 seconds. The catalogue reads this
-- instead; the sync keeps it current as it handles each product anyway.

create table if not exists public.store_catalogue_products (
  merchant_record_id text not null,
  shopify_product_id text not null,
  sku text,                      -- normalised: upper case, no whitespace
  name text,
  brand text,
  picture_url text,
  status text not null default 'active',
  sizes integer not null default 0,
  price_low numeric,             -- the store's own price, lowest variant
  price_high numeric,
  updated_at timestamptz not null default now(),
  primary key (merchant_record_id, shopify_product_id)
);

create index if not exists store_catalogue_products_list_idx
  on public.store_catalogue_products (merchant_record_id, status, name);

create index if not exists store_catalogue_products_sku_idx
  on public.store_catalogue_products (merchant_record_id, sku);

-- Written by the sync. A row is only written when something in it changed.
create or replace function public.upsert_catalogue_products_changed(rows jsonb)
returns integer
language plpgsql
as $function$
declare
  written integer;
begin
  insert into store_catalogue_products as c (
    merchant_record_id, shopify_product_id, sku, name, brand, picture_url,
    status, sizes, price_low, price_high, updated_at
  )
  select
    r->>'merchant_record_id', r->>'shopify_product_id', r->>'sku', r->>'name', r->>'brand',
    r->>'picture_url', coalesce(r->>'status', 'active'), coalesce((r->>'sizes')::int, 0),
    (r->>'price_low')::numeric, (r->>'price_high')::numeric, now()
  from jsonb_array_elements(rows) r
  on conflict (merchant_record_id, shopify_product_id) do update set
    sku = excluded.sku,
    name = excluded.name,
    brand = excluded.brand,
    picture_url = excluded.picture_url,
    status = excluded.status,
    sizes = excluded.sizes,
    price_low = excluded.price_low,
    price_high = excluded.price_high,
    updated_at = now()
  where (c.sku, c.name, c.brand, c.picture_url, c.status, c.sizes, c.price_low, c.price_high)
        is distinct from
        (excluded.sku, excluded.name, excluded.brand, excluded.picture_url, excluded.status,
         excluded.sizes, excluded.price_low, excluded.price_high);

  get diagnostics written = row_count;
  return written;
end;
$function$;

-- A product switched off (draft, archived, deleted, unseen in a full pass).
create or replace function public.deactivate_catalogue_products(p_merchant text, p_product_ids text[])
returns integer
language plpgsql
as $function$
declare
  written integer;
begin
  update store_catalogue_products
     set status = 'inactive', updated_at = now()
   where merchant_record_id = p_merchant
     and shopify_product_id = any (p_product_ids)
     and status <> 'inactive';

  get diagnostics written = row_count;
  return written;
end;
$function$;

-- One store at a time, run once to fill the table from store_listings.
-- Heavy for a big store (it reads all of its rows), so run it at night.
create or replace function public.backfill_catalogue_products(p_merchant text)
returns integer
language plpgsql
set statement_timeout = '15min'
as $function$
declare
  written integer;
begin
  insert into store_catalogue_products as c (
    merchant_record_id, shopify_product_id, sku, name, brand, picture_url,
    status, sizes, price_low, price_high, updated_at
  )
  select
    l.merchant_record_id,
    l.shopify_product_id,
    min(upper(regexp_replace(coalesce(l.sku, ''), '\s', '', 'g'))),
    coalesce(min(l.shopify_product_name), min(l.stockx_product_name)),
    min(l.brand),
    min(l.picture_url),
    case when bool_or(l.status = 'active') then 'active' else 'inactive' end,
    count(*) filter (where l.status = 'active'),
    min(l.store_price),
    max(l.store_price),
    now()
  from store_listings l
  where l.merchant_record_id = p_merchant
  group by l.merchant_record_id, l.shopify_product_id
  on conflict (merchant_record_id, shopify_product_id) do nothing;

  get diagnostics written = row_count;
  return written;
end;
$function$;

-- 2. What a store chose about our stock --------------------------------------
--
--   add          create the product in their Shopify (photos: ours or theirs)
--   unlinked     keep their product, stop putting our stock on it
--   deactivated  stop our stock and set the product to draft in Shopify
--
-- No row means the default: an existing product is filled, nothing new is
-- created. Deleting the row undoes a choice.

create table if not exists public.store_catalogue_choices (
  merchant_record_id text not null,
  sku text not null,             -- normalised: upper case, no whitespace
  choice text not null check (choice in ('add', 'unlinked', 'deactivated')),
  photos text check (photos in ('lojiq', 'own')),
  status text not null default 'pending' check (status in ('pending', 'done', 'failed')),
  error text,
  decided_at timestamptz not null default now(),
  done_at timestamptz,
  primary key (merchant_record_id, sku)
);

-- 3. When a SKU first appeared in consignment --------------------------------
--
-- For the "New" tab: stock that showed up in the last fourteen days. Kept in
-- its own table because consignment rows come and go as pairs sell.

create table if not exists public.consignment_sku_first_seen (
  sku text primary key,          -- normalised: upper case, no whitespace
  first_seen_at timestamptz not null default now()
);

insert into public.consignment_sku_first_seen (sku, first_seen_at)
select upper(regexp_replace(coalesce(sku, ''), '\s', '', 'g')), min(created_at)
from public.consignment_inventory
where coalesce(sku, '') <> ''
group by 1
on conflict (sku) do nothing;

create or replace function public.remember_consignment_sku()
returns trigger
language plpgsql
as $function$
begin
  if coalesce(new.sku, '') <> '' then
    insert into public.consignment_sku_first_seen (sku)
    values (upper(regexp_replace(new.sku, '\s', '', 'g')))
    on conflict (sku) do nothing;
  end if;

  return new;
end;
$function$;

drop trigger if exists consignment_sku_first_seen_trg on public.consignment_inventory;

create trigger consignment_sku_first_seen_trg
after insert on public.consignment_inventory
for each row execute function public.remember_consignment_sku();

-- 4. The "Add" tab: our consignment SKUs this store does not carry -----------
--
-- Read from the small tables only: consignment_stock_levels (one row per
-- SKU and size) against store_catalogue_products (one row per product).
-- New = first seen in consignment within the last fourteen days.

create or replace function public.store_catalogue_to_add(
  p_merchant text,
  p_search text default null,
  p_new_only boolean default false,
  p_limit integer default 60,
  p_offset integer default 0
)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $function$
  with stock as (
    select upper(regexp_replace(coalesce(sku, ''), '\s', '', 'g')) as sku,
           min(product_name) as name,
           min(brand) as brand,
           count(*) as sizes,
           sum(stock_level) as pairs,
           min(lowest_suggested_price) as price_low,
           max(lowest_suggested_price) as price_high
    from consignment_stock_levels
    where stock_level > 0
    group by 1
  ),
  pictures as (
    select upper(regexp_replace(coalesce(sku, ''), '\s', '', 'g')) as sku, min(image_url) as picture
    from consignment_inventory
    where quantity > 0 and coalesce(image_url, '') <> ''
    group by 1
  ),
  candidates as (
    select s.*, p.picture, f.first_seen_at,
           (f.first_seen_at > now() - interval '14 days') as is_new,
           ch.choice, ch.photos, ch.status as choice_status
    from stock s
    left join pictures p on p.sku = s.sku
    left join consignment_sku_first_seen f on f.sku = s.sku
    left join store_catalogue_choices ch on ch.merchant_record_id = p_merchant and ch.sku = s.sku
    where s.sku <> ''
      and not exists (
        select 1 from store_catalogue_products c
        where c.merchant_record_id = p_merchant and c.sku = s.sku
      )
      and (p_search is null or btrim(p_search) = ''
           or s.sku ilike '%' || btrim(p_search) || '%'
           or s.name ilike '%' || btrim(p_search) || '%'
           or s.brand ilike '%' || btrim(p_search) || '%')
      and (not p_new_only or f.first_seen_at > now() - interval '14 days')
  )
  select jsonb_build_object(
    'total', (select count(*) from candidates),
    'items', coalesce((
      select jsonb_agg(to_jsonb(x))
      from (
        select * from candidates
        order by is_new desc nulls last, name nulls last, sku
        limit greatest(1, least(p_limit, 200)) offset greatest(0, p_offset)
      ) x
    ), '[]'::jsonb)
  );
$function$;
