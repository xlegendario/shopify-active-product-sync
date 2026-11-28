-- Autopricing tab, 28-11-2026: each size also carries its store_listings id
-- and price_mode, so a store can set "Keep my price" (price_mode own) on a
-- size from test mode, before it puts everything live.

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
            'id', s.id,
            'size', s.size,
            'price_mode', s.price_mode,
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
