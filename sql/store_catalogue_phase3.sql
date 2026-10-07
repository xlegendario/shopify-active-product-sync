-- Phase 3 of the store catalogue (07-10-2026). Run once, in one go.
--
-- 1. A third price mode, 'own': the store prices that size itself, even
--    with Price Sync on. We leave the number alone and judge the pair
--    against it, as for a store without Price Sync.
-- 2. UNION Amsterdam's existing products count as added. Product Sync
--    built UNION's whole catalogue; with that switch gone, a page only gets
--    new sizes when the store added it, so without this UNION's pages
--    would stop growing.

alter table public.store_listings drop constraint if exists store_listings_price_mode_check;
alter table public.store_listings add constraint store_listings_price_mode_check
  check (price_mode = any (array['auto'::text, 'custom'::text, 'own'::text]));

create or replace function public.store_shelf_set_price(
  p_merchant text,
  p_sku text default null,
  p_id bigint default null,
  p_mode text default 'auto',
  p_price numeric default null
)
returns jsonb
language plpgsql
security definer
set search_path to 'public'
as $function$
declare
  v_changed integer;
begin
  if p_mode not in ('auto', 'custom', 'own') then
    raise exception 'mode is auto, custom or own';
  end if;

  if p_mode = 'custom' and coalesce(p_price, 0) <= 0 then
    raise exception 'a custom price needs an amount';
  end if;

  update public.store_listings l
     set price_mode   = p_mode,
         custom_price = case when p_mode = 'custom' then round(p_price, 2) else null end,
         price_set_at = now(),
         updated_at   = now()
   where l.merchant_record_id = p_merchant
     and (
       (p_id is not null and l.id = p_id)
       or (
         p_id is null
         and p_sku is not null
         and l.shopify_price is not null
         and upper(regexp_replace(coalesce(l.sku, ''), '\s', '', 'g'))
             = upper(regexp_replace(p_sku, '\s', '', 'g'))
       )
     );

  get diagnostics v_changed = row_count;

  return jsonb_build_object('changed', v_changed);
end;
$function$;

insert into public.store_catalogue_choices (merchant_record_id, sku, choice, photos, status, decided_at, done_at)
select distinct 'recYJgfQMAAXkrY4Q', upper(regexp_replace(sku, '\s', '', 'g')), 'add', 'lojiq', 'done', now(), now()
from public.store_listings
where merchant_record_id = 'recYJgfQMAAXkrY4Q'
  and coalesce(btrim(sku), '') <> ''
on conflict (merchant_record_id, sku) do nothing;

select count(*) as union_products_marked_added
from public.store_catalogue_choices
where merchant_record_id = 'recYJgfQMAAXkrY4Q' and choice = 'add';
