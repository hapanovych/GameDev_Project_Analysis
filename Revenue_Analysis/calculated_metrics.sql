with grouped_table as (
	select date(date_trunc('month', payment_date)) as month,
		user_id,
		game_name,
		sum(revenue_amount_usd) as revenue_mrr -- metric 1 - Monthly Recurring Revenue (MRR)
	from project.games_payments gp
	group by month, user_id, game_name
	order by 1, 3
	),
-- auxiliary calendar table 
calendar_table as (
	select *,  
		date(month - interval '1 month') as previous_month,
		date(month + interval '1 month') as following_month,
		lag(revenue_mrr)
			over (partition by user_id order by month) as previous_payment,
		lag(month)
			over (partition by user_id order by month) as previous_payment_month,
		lead(month)
			over (partition by user_id order by month) as following_payment_month
	from grouped_table
),
lifetime_metrics as (
	select 
		user_id,
		(max(payment_date)::date - min(payment_date)::date) as life_time,
		sum(revenue_amount_usd) as life_time_revenue
	from project.games_payments gp
	group by user_id
)
select month,
	user_id,
	ct.game_name,
	revenue_mrr,
	case
		when previous_payment is null
		then revenue_mrr
	end as new_mrr,
	case
		when following_payment_month is null
		then revenue_mrr
	end as churned_revenue,
	case
		when revenue_mrr > previous_payment
		then revenue_mrr - previous_payment
	end as expansion_mrr,
	case
		when revenue_mrr < previous_payment
		then revenue_mrr - previous_payment
	end as contraction_mrr,
	life_time,
	life_time_revenue,
	gpu.language,
	gpu.has_older_device_model,
	gpu.age
from calendar_table ct
left join lifetime_metrics using(user_id)
left join project.games_paid_users gpu using(user_id)
;

