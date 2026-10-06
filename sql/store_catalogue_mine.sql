-- The "My catalogue" tab: a store's own products, with how much of each we
-- hold in consignment and what the store chose about it. Adds one function.
--
-- Read from the small tables only: store_catalogue_products (one row per
-- product), consignment_stock_levels and store_catalogue_choices. Active
-- products first, inactive at the bottom.

create or replace function public.store_catalogue_mine(
  p_merchant text,
  p_search text default null,
  p_status text default null,      -- 'active' | 'inactive' | null for both
  p_ours text default null,        -- 'yes' (we hold stock) | 'no' | null
  p_limit integer default 60,
  p_offset integer default 0
)
returns jsonb
language sql
stable
security definer
set search_path to 'public'
as $function$
  with ours as (
    select upper(regexp_replace(coalesce(sku, ''), '\s', '', 'g')) as sku,
           count(*) as our_sizes,
           sum(stock_level) as our_pairs
    from consignment_stock_levels
    where stock_level > 0
    group by 1
  ),
  picked as (
    select c.*, o.our_sizes, o.our_pairs,
           ch.choice, ch.photos, ch.status as choice_status
    from store_catalogue_products c
    left join ours o on o.sku = c.sku
    left join store_catalogue_choices ch
      on ch.merchant_record_id = c.merchant_record_id and ch.sku = c.sku
    where c.merchant_record_id = p_merchant
      and (p_status is null or c.status = p_status)
      and (p_ours is null
           or (p_ours = 'yes' and o.sku is not null)
           or (p_ours = 'no' and o.sku is null))
      and (p_search is null or btrim(p_search) = ''
           or c.sku ilike '%' || btrim(p_search) || '%'
           or c.name ilike '%' || btrim(p_search) || '%'
           or c.brand ilike '%' || btrim(p_search) || '%')
  )
  select jsonb_build_object(
    'total', (select count(*) from picked),
    'active', (select count(*) from picked where status = 'active'),
    'items', coalesce((
      select jsonb_agg(to_jsonb(x))
      from (
        select * from picked
        order by (status = 'active') desc, name nulls last, shopify_product_id
        limit greatest(1, least(p_limit, 200)) offset greatest(0, p_offset)
      ) x
    ), '[]'::jsonb)
  );
$function$;
