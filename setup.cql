create KEYSPACE users WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use users;

create table user (
    user_id int primary key,
    name text,
    age int,
    favorite_foods set<text>
);

insert into user (user_id, name, age, favorite_foods) VALUES (1, 'Jon', 34, {'Bacon', 'Cheese'});
insert into user (user_id, name, age, favorite_foods) VALUES (2, 'Dani', 22, {'Wine', 'Kale', 'Pizza'});
insert into user (user_id, name, age, favorite_foods) VALUES (3, 'Patrick', 108, {'Pie', 'Steak', 'Muffins'});
insert into user (user_id, name, age, favorite_foods) VALUES (4, 'Baby Luke', 1, {'Candy', 'Fear'});
insert into user (user_id, name, age, favorite_foods) VALUES (5, 'Larry', 10, {'Anger'});

create table favorite_foods_index (
    food text,
    user_id int,
    primary key(food, user_id)
);

create keyspace movielens WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
use movielens;

create table movie (
    movie_id int primary key,
    name text,
    year int,
    tags set<text>
);

create table rating_by_movie (
    movie_id int,
    user_id int,
    rating int,
    timestamp int,
    primary key(movie_id, user_id)
);
