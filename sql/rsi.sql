drop type if exists RSI_STATE_SMA;
create TYPE RSI_STATE_SMA AS (step integer,prev_close numeric,avg_gain numeric, avg_loss numeric);
CREATE OR replace FUNCTION rsi_sma_accum (state RSI_STATE_SMA, num_elem numeric) -- performs accumulated sum
RETURNS RSI_STATE_SMA

AS $$
DECLARE 
	
BEGIN
	if state.prev_close > 0 then
		if num_elem > state.prev_close then
			state.avg_gain := state.avg_gain + (num_elem - state.prev_close);
		 	state.avg_loss := state.avg_loss;
		else
			state.avg_loss := state.avg_loss +state.prev_close - num_elem;
			state.avg_gain := state.avg_gain;
		end if;	
	
	end if; 
	
	state.step := state.step + 1;
	state.prev_close := num_elem;

    return state;
END; 


$$ LANGUAGE plpgsql;



CREATE OR replace FUNCTION rsi_sma_final (state RSI_STATE_SMA) -- performs devision and returns final value
RETURNS NUMERIC
AS $$
declare 
	avg_g numeric;
	avg_l numeric;
	rs numeric;
begin 
	avg_g := state.avg_gain/state.step;
	avg_l := state.avg_loss/state.step;

	if avg_l = 0 then 
		rs := 100 ;
	else
		rs :=   (100 - (100/(1+ ( (avg_g  / avg_l )))));
	end if;
	
 	return rs;
end;


$$ LANGUAGE plpgsql;


CREATE or replace AGGREGATE rsi_sma(numeric) ( -- NUMERIC is what the function returns
    initcond = '(0,0,0,0)', -- this is the initial state of type POINT
    stype = RSI_STATE_SMA, -- this is the type of the state that will be passed between steps
    sfunc = rsi_sma_accum, -- this is the function that knows how to compute a new average from existing average and new element. Takes in the state (type POINT) and an element for the step (type NUMERIC)
    finalfunc =  rsi_sma_final -- returns the result for the aggregate function. Takes in the state of type POINT (like all other steps) and returns the result as what the aggregate function returns - NUMERIC 
);

rsi_sma({column_name}) over(partition by {partition_by_column} order by {order_by_column_asc} asc rows between {duration_in_days} preceding and current row) as {name}