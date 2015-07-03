create KEYSPACE users WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use users;

create table user (
    user_id int primary key,
    name text,
    favorite_foods set<text>
);

insert into user (user_id, name, favorite_foods) VALUES (1, 'Jon', {'Bacon', 'Cheese'});
insert into user (user_id, name, favorite_foods) VALUES (2, 'Dani', {'Wine', 'Kale', 'Pizza'});
insert into user (user_id, name, favorite_foods) VALUES (3, 'Patrick', {'Pie', 'Steak', 'Muffins'});

create table favorite_foods_index (
    food text,
    user_id int,
    primary key(food, user_id)
);

create keyspace movielens;
use movielens;

create table movie (
    movie_id int primary key,
    name text,
    year int,
    tags set<text>
);

create table user (

)
