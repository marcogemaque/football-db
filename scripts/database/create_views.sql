CREATE MATERIALIZED VIEW updated_ranking_table
AS select 
	* 
from (
	select
		home_team_uuid,
		sum(home_team_points) as total_points,
		sum(goals_home) as goals_favour,
		sum(goals_away) as goals_against
	from (
		select 
			home_team_uuid,
			sum(goals_home) as goals_home,
			sum(goals_away) as goals_away,
			sum(case 
				when goals_home > goals_away then 3
				when goals_home = goals_away then 1
				else 0
			end) as home_team_points
		from 
			fixture f
		group by 
			home_team_uuid
		union
		select 
			away_team_uuid,
			sum(goals_away),
			sum(goals_home),
			sum(case 
				when goals_home > goals_away then 0
				when goals_home = goals_away then 1
				else 3
			end) as away_team_points
		from 
			fixture f
		group by 
			away_team_uuid) as subquery
	group by 
		home_team_uuid) big_query
left join team_keys tk on big_query.home_team_uuid = tk.uuid
order by 
	total_points desc,
	goals_favour desc