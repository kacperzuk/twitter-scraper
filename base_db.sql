create type cmd_method as enum ('get', 'post');

create table cmd_queue (
    id serial primary key,
    method cmd_method not null,
    path text not null,
    params json,
    tag text not null,
    is_processing_since timestamp
);

create table res_queue (
    id serial primary key,
    tag text not null,
    result json
);
