-- Gran Villagio Mini Bar
-- Base de persistencia para a versao de testes.
-- O servidor conecta usando a credencial privada do PostgreSQL (DATABASE_URL).
-- Nao habilitar acesso anonimo/authenticated a esta tabela.

create table if not exists public.app_state (
  id smallint primary key default 1 check (id = 1),
  payload jsonb not null,
  updated_at timestamptz not null default now()
);

alter table public.app_state enable row level security;

revoke all on table public.app_state from anon, authenticated;
grant all on table public.app_state to postgres;

comment on table public.app_state is
  'Estado operacional persistido do sistema de minibar. Acesso exclusivo do backend.';
comment on column public.app_state.payload is
  'Documento JSON do estado operacional, incluindo cadastros e registros historicos.';
