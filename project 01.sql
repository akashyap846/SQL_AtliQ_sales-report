SELECT m.date,m.product_code,m.sold_quantity, 
		p.product, p.variant,
        g.gross_price,
        round(g.gross_price* m.sold_quantity,2) as gross_price_total
FROM fact_sales_monthly m
join dim_product p
on p.product_code=m.product_code
join fact_gross_price g
on g.product_code=m.product_code and g.fiscal_year=get_fiscal_year(m.date)
where customer_code ="90002002" and 
get_fiscal_year(date)="2021"
order by date;



-- second query 

SELECT m.date, 
        sum(round(g.gross_price * m.sold_quantity,2)) as gross_price_total
FROM fact_sales_monthly m
join fact_gross_price g
on g.product_code=m.product_code and g.fiscal_year=get_fiscal_year(m.date)
where customer_code ="90002002"
group by m.date;



-- yearly report 

SELECT get_fiscal_year(m.date), 
        sum(round(g.gross_price * m.sold_quantity,2)) as yearly_sales
FROM fact_sales_monthly m
join fact_gross_price g
on g.product_code=m.product_code and g.fiscal_year=get_fiscal_year(m.date)
join dim_customer c
on c.customer_code = m.customer_code
where m.customer_code ="90002002" and c.market = "India"
group by get_fiscal_year(m.date);

-- same query but different way 
select
            get_fiscal_year(date) as fiscal_year,
            sum(round(sold_quantity*g.gross_price,2)) as yearly_sales
	from fact_sales_monthly s
	join fact_gross_price g
	on 
	    g.fiscal_year=get_fiscal_year(s.date) and
	    g.product_code=s.product_code
	where
	    customer_code=90002002
	group by get_fiscal_year(date)
	order by fiscal_year;





-- printing the pre invoice deduction and make it as view 

SELECT m.date,m.product_code,m.sold_quantity,  
		c.market,
		p.product, p.variant,
        g.gross_price,
        round(g.gross_price* m.sold_quantity,2) as gross_price_total,
        pre.pre_invoice_discount_pct
FROM fact_sales_monthly m
join dim_customer c
on c.customer_code = m.customer_code
join dim_product p
on p.product_code=m.product_code
join fact_gross_price g
on g.product_code=m.product_code and g.fiscal_year=m.fiscal_year
join fact_pre_invoice_deductions pre
on pre.customer_code=m.customer_code and pre.fiscal_year=m.fiscal_year
order by date;


-- Using views to create a table

select *, (gross_price_total - gross_price_total*pre_invoice_discount_pct) as net_invoice,
(po.discounts_pct+po.other_deductions_pct) as post_invoice_discount_pct
from sales_pre_invoice_discount so
join fact_post_invoice_deductions po
on so.date= po.date 
and so.customer_code  = po.customer_code 
and so.product_code = po.product_code;



-- group by market and created the store procedure 

		SELECT 
			market, ROUND(SUM(net_sales) / 1000000, 2) AS net_sales_mln
		FROM
			gdb0041.net_sales_view
		WHERE
			fiscal_year = 2021
		GROUP BY market
		ORDER BY net_sales_mln DESC
		LIMIT 5;


-- group by top customer and created the store procedure 

	SELECT 
			c.customer, ROUND(SUM(net_sales) / 1000000, 2) AS net_sales_mln
		FROM
			gdb0041.net_sales_view n
            join dim_customer c
            on c.customer_code = n.customer_code
		WHERE
			fiscal_year = 2021
		GROUP BY c.customer
		ORDER BY net_sales_mln DESC
		LIMIT 5;
        
        
-- creating a window function to get the report on basis of customer and net sales over a same window

with cte1 as 
( SELECT 
			c.customer, ROUND(SUM(net_sales)/1000000,2) AS net_sales_mln
		FROM
			gdb0041.net_sales_view n
            join dim_customer c
            on c.customer_code = n.customer_code
		WHERE
			fiscal_year = 2021
		GROUP BY c.customer
        )
	SELECT 
    *,
    net_sales_mln*100/SUM(net_sales_mln) over() AS net_sales_pct
FROM
    cte1
    group by customer
ORDER BY net_sales_mln DESC;



-- creating the report customer and region wise 

with cte_two as(
SELECT 
			c.customer,c.region, ROUND(SUM(net_sales)/1000000,2) AS net_sales_mln
		FROM
			gdb0041.net_sales_view n
            join dim_customer c
            on c.customer_code = n.customer_code
		WHERE
			fiscal_year = 2021
		GROUP BY c.customer,c.region
        )
        SELECT 
    *,
    net_sales_mln*100/SUM(net_sales_mln) over(partition by region) AS net_sales_pct
FROM
    cte_two
ORDER BY region,net_sales_mln DESC;



-- get_top_n_product per division by qty sold

with cte1 as (
select p.division,
p.product,
sum(sold_quantity) as total_qty
from fact_sales_monthly m
join dim_product p
on p.product_code = m.product_code
where fiscal_year =2021
group by p.product,p.division),
	cte2 as (
select *, dense_rank() over(partition by division order by total_qty desc) as drnk
from cte1
)
select * from cte2 where drnk<=3;



### Module: Create a Helper Table

-- Create fact_act_est table
	drop table if exists fact_act_est;

	create table fact_act_est
	(
        	select 
                    s.date as date,
                    s.fiscal_year as fiscal_year,
                    s.product_code as product_code,
                    s.customer_code as customer_code,
                    s.sold_quantity as sold_quantity,
                    f.forecast_quantity as forecast_quantity
        	from 
                    fact_sales_monthly s
        	left join fact_forecast_monthly f 
        	using (date, customer_code, product_code)
	)
	union
	(
        	select 
                    f.date as date,
                    f.fiscal_year as fiscal_year,
                    f.product_code as product_code,
                    f.customer_code as customer_code,
                    s.sold_quantity as sold_quantity,
                    f.forecast_quantity as forecast_quantity
        	from 
		    fact_forecast_monthly  f
        	left join fact_sales_monthly s 
        	using (date, customer_code, product_code)
	);

	update fact_act_est
	set sold_quantity = 0
	where sold_quantity is null;

	update fact_act_est
	set forecast_quantity = 0
	where forecast_quantity is null;
    

    
    
    ### Module: Temporary Tables & Forecast Accuracy Report

-- Forecast accuracy report using cte (It exists at the scope of statements)
	with forecast_err_table as (
             select
                  s.customer_code as customer_code,
                  c.customer as customer_name,
                  c.market as market,
                  sum(s.sold_quantity) as total_sold_qty,
                  sum(s.forecast_quantity) as total_forecast_qty,
                  sum(1.0*s.forecast_quantity-s.sold_quantity) as net_error,
                  round(sum(1.0*s.forecast_quantity-s.sold_quantity)*100/sum(s.forecast_quantity),1) as net_error_pct,
                  sum(abs(1.0*s.forecast_quantity-s.sold_quantity)) as abs_error,
                  round(sum(abs(1.0*s.forecast_quantity-sold_quantity))*100/sum(s.forecast_quantity),2) as abs_error_pct
             from fact_act_est s
             join dim_customer c
             on s.customer_code = c.customer_code
             where s.fiscal_year=2021
             group by customer_code
	)
	select 
            *,
            if (abs_error_pct > 100, 0, 100.0 - abs_error_pct) as forecast_accuracy
	from forecast_err_table
        order by forecast_accuracy desc;

select
customer_code as customer_code,sum(forecast_quantity-sold_quantity) as net_error
from fact_act_est 
group by customer_code;
