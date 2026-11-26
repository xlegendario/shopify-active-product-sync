-- Autopricing with the store's own margins (26-11-2026).
--
-- autoprice_settings: one row per store that takes autopricing, with its
-- margin bands as [{ "from": 0, "margin": 55 }, { "from": 100, "margin": 49.5 }, ...].
-- "from" is the cost basis in euros (StockX incl. VAT, GOAT incl. 15 shipping),
-- separate from the Firm Range in Merchants, which stays the order limit.
--
-- autoprice_live: the products a store put live from test mode. Only these
-- get their autopricing price written to Shopify.
--
-- autoprice_listings.basis: the cost the price was built on, so the portal
-- can show what a margin change does before the next run.

create table if not exists public.autoprice_settings (
  merchant_record_id text primary key,
  margin_bands jsonb not null default '[]'::jsonb,
  updated_at timestamptz not null default now()
);

create table if not exists public.autoprice_live (
  merchant_record_id text not null,
  sku text not null,
  live_since timestamptz not null default now(),
  primary key (merchant_record_id, sku)
);

alter table public.autoprice_listings add column if not exists basis numeric;

-- Only the service role (sync service, portal server) reads and writes these.
alter table public.autoprice_settings enable row level security;
alter table public.autoprice_live enable row level security;

-- SNEAKEREGEER (CL-00035): start margins fitted on his own price table, as
-- they are (no +1,50; Dario 26-11-2026). He can change them in the portal.
insert into public.autoprice_settings (merchant_record_id, margin_bands)
values ('rechUAQ4tLIp19QV9', '[
  {"from": 0,   "margin": 55},
  {"from": 100, "margin": 49.5},
  {"from": 225, "margin": 54.5},
  {"from": 250, "margin": 59.5},
  {"from": 275, "margin": 69.5},
  {"from": 325, "margin": 79.5},
  {"from": 425, "margin": 89.5},
  {"from": 500, "margin": 99.5},
  {"from": 575, "margin": 109.5},
  {"from": 650, "margin": 119.5},
  {"from": 725, "margin": 129.5},
  {"from": 800, "margin": 139.5},
  {"from": 875, "margin": 149.5},
  {"from": 900, "margin": 159.5}
]'::jsonb)
on conflict (merchant_record_id) do nothing;

-- The Autopricing tab in the portal (Product Catalog): one page of the
-- store's active products, every size in its shop with its shop price, and
-- what autopricing has for it (basis, last price). The portal works the
-- "will be" price out of basis + the store's current margins, so a margin
-- change shows at once.
create or replace function public.autoprice_preview(
  p_merchant text,
  p_search text default null,
  p_live text default null,        -- 'yes' | 'no' | null for both
  p_limit integer default 40,
  p_offset integer default 0
)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $$
  with prods as (
    select c.sku, c.name, c.picture_url, c.shopify_product_id,
           exists (select 1 from autoprice_live l where l.merchant_record_id = p_merchant and l.sku = upper(c.sku)) as live
    from store_catalogue_products c
    where c.merchant_record_id = p_merchant
      and c.status = 'active'
      and (p_search is null or c.sku ilike '%' || p_search || '%' or c.name ilike '%' || p_search || '%')
  ),
  filtered as (
    select * from prods where p_live is null or live = (p_live = 'yes')
  ),
  page as (
    select * from filtered
    order by live desc, name nulls last, sku
    limit greatest(least(p_limit, 100), 1) offset greatest(p_offset, 0)
  )
  select jsonb_build_object(
    'total', (select count(*) from filtered),
    'live', (select count(*) from autoprice_live where merchant_record_id = p_merchant),
    'items', coalesce((
      select jsonb_agg(jsonb_build_object(
        'sku', upper(p.sku), 'name', p.name, 'picture_url', p.picture_url, 'live', p.live,
        'sizes', (
          select coalesce(jsonb_agg(jsonb_build_object(
            'size', s.size,
            'shop_price', coalesce(s.store_price, s.shopify_price),
            'custom_price', s.custom_price,
            'status', a.status, 'source', a.source, 'basis', a.basis, 'price', a.price
          )), '[]'::jsonb)
          from store_listings s
          left join autoprice_listings a
            on a.merchant_record_id = p_merchant and a.sku = upper(p.sku) and a.size = s.size
          -- On the product id: idx_store_listings_lookup. Matching on sku
          -- here took 20 s for 40 products (no usable index), this 13 ms.
          where s.merchant_record_id = p_merchant
            and s.shopify_product_id = p.shopify_product_id
        )
      ) order by p.live desc, p.name nulls last, p.sku)
      from page p
    ), '[]'::jsonb)
  );
$$;
