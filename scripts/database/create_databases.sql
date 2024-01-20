create table if not exists team_keys (
	uuid varchar(36) primary key,
	team_name varchar(200) not null,
	country varchar(200) not null
)

create table if not exists teams_aliases (
	uuid varchar(36),
	alias text[],
	primary key (uuid),
	foreign key (uuid)
		references team_keys (uuid)
);

create table if not exists scrape_urls (
	uuid varchar(36) not null,
	type_of_url varchar(20) not null,
	url text,
	primary key (uuid),
	foreign key (uuid)
		references team_keys (uuid)
);

create table if not exists player_stats (
	uuid varchar(36) not null,
	player_number varchar(100),
	player_position varchar(50) not null,
	age int not null,
	matches_played int not null,
	goals int not null,
	assists int not null,
	yellow_cards int not null,
	double_yellows int not null,
	red_cards int not null,
	subbed_in int not null,
	subbed_out int not null,
	points_per_game numeric(3,2) not null,
	minutes_played numeric(6,1),
	player_name varchar(50),
	primary key (uuid),
	foreign key (uuid)
		references team_keys (uuid)
);

create table if not exists team_value (
	uuid varchar(36) not null,
	value numeric(6) not null,
	primary key (uuid)
	foreign key (uuid)
		references team_keys (uuid)
)


create table if not exists fixture (
	match_id serial primary key, --the sequential id
	home_team_uuid varchar(36) not null, --team we're referring as main
	away_team_uuid varchar(36) not null, --team played against
	goals_home int not null, --goals for the main team
	goals_against int not null, --goals against the team
	match_day date not null,
	competition varchar(36) not null, --national league, cup, libertadores, etc,
	country_competition varchar(36) not null
)