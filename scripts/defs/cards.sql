create table cards (
    id text primary key,
    set_names text not null,
    title text unique not null,
    stripped_title text unique not null,
    side_id text not null,
    faction_id text not null,
    influence_cost int,
    card_type_id text not null,
    card_subtype_ids text not null,
    cost int,
    trash_cost int,
    -- agendas only
    advancement_requirement int,
    agenda_points int,
    -- programs/ice only
    strength int,
    memory_cost int
)  


