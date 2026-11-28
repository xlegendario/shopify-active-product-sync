-- Autopricing switched on with Autopricing? in Merchants (28-11-2026).
--
-- A store ticked for the first time gets a row in autoprice_settings from the
-- sync service or the portal, with only its record id: the margins then come
-- from this column default. For now the default is the band table fitted for
-- SNEAKEREGEER; change it here when the standard changes (rows already made
-- keep their own margins).

alter table public.autoprice_settings
  alter column margin_bands set default '[
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
  ]'::jsonb;
