from pylivetrader.api import (
    attach_pipeline,
    date_rules,
    time_rules,
    order,
	order_target_percent,
    get_open_orders,
    cancel_order,
    pipeline_output,
    schedule_function,
)

import numpy as np
import pandas as pd
#API imports for pipeline
from pylivetrader.finance.execution import LimitOrder
from zipline.pipeline import Pipeline
#from quantopian.pipeline import Pipeline
#from quantopian.algorithm import attach_pipeline, pipeline_output
from pipeline_live.data.iex.pricing import USEquityPricing
#from quantopian.pipeline.data.builtin import USEquityPricing
from pipeline_live.data.polygon.filters import (
    IsPrimaryShareEmulation as IsPrimaryShare)
#from quantopian.pipeline.filters.morningstar import IsPrimaryShare #,Q3000US, QTradableStocksUS
from pipeline_live.data.iex.factors import (
    AnnualizedVolatility
)
#from quantopian.pipeline.factors import AnnualizedVolatility
import logbook
log = logbook.Logger('algo')

def record(*args, **kwargs):
    print('args={}, kwargs={}'.format(args, kwargs))
    log.info("START TEST")

def initialize (context): # runs once when script starts
    #context is a python dictionary that contains information on portfolio/performance.
    context.idr_losers = pd.Series(([]))
    context.day_count = 0
    context.daily_message = "Day {}."
    context.open_orders = get_open_orders()
    context.backup_stocks = symbols('VTI')

    #Factor criteria
    #dolvol = AverageDollarVolume(window_length = 1)
    close_price = USEquityPricing.close.latest
    vol = USEquityPricing.volume.latest
    ann_var = AnnualizedVolatility()
    #daily_return = DailyReturns([USEquityPricing.close], window_length = 2) #-- going to use history instead of pipeline
    
    #screening
    mask_custom = (IsPrimaryShare() & (vol < 200000) & (close_price > 1) & (close_price < 3) & (ann_var > 0.815)) # Q3000US(8000000) &
    stockBasket = USEquityPricing.close.latest.top(3000,  mask = mask_custom)
    
    #Column construction
    pipe_columns = {'close_price': close_price, "volume": vol, 'ann_var': ann_var}
    
    #Ceation of actual pipeline
    pipe = Pipeline(columns = pipe_columns, screen = stockBasket)
    attach_pipeline(pipe, "Stocks")

    #Schedule functions
    schedule_function(late_day_trade, date_rules.every_day(), time_rules.market_open(hours = 5, minutes = 56)) #offset open tells when to run a user defined function
    schedule_function(check_portfolio, date_rules.every_day(), time_rules.market_open(hours = 0, minutes = 1))
    schedule_function(morning_day_trade1, date_rules.every_day(), time_rules.market_open(hours = 0, minutes = 15))
    #schedule_function(check_portfolio, date_rules.every_day(), time_rules.market_open(hours = 0, minutes = 16))
    schedule_function(morning_day_trade2, date_rules.every_day(), time_rules.market_open(hours = 0, minutes = 45))
    
    #schedule_function(morning_day_trade3, date_rules.every_day(), time_rules.market_open(hours = 2, minutes = 0))
    #schedule_function(check_portfolio, date_rules.every_day(), time_rules.market_open(hours = 0, minutes = 48))
    
def late_day_trade(context, data):
    #Get the pipeline output
    pipe_output = pipeline_output('Stocks')
    context.days_stocks = pipe_output.sort_values(by =['ann_var'], ascending = False)
    #log.info(context.days_stocks)
    log.info(context.daily_message, context.day_count)
    log.info(context.days_stocks)
    log.info(type(context.days_stocks))
    
    #Calculate Daily Return Top Losers
    if (context.days_stocks.size > 0):
        price_history = data.history(context.days_stocks.index, "price", 745, "1m") #356 +390
        open_prices = price_history.iloc[0]
        current_prices = price_history.iloc[-1]
        context.idr_losers = ((current_prices - open_prices) / open_prices).sort_values()
        context.idr_losers = context.idr_losers[0:5]#5    
        log.info(context.idr_losers)
    else:
        price_history = data.history(context.backup_stocks, "price", 1 , "1m") #356
        current_prices = price_history.iloc[-1]
        context.idr_losers = current_prices #Stock info is irrelevant here  
          
    pct_cash = context.portfolio.cash/context.portfolio.portfolio_value
    
    #Get Open Orders and Buy
    for stock in context.idr_losers.index:
        if(data.can_trade(stock)):
            if(stock not in context.open_orders):
               order_target_percent(stock, pct_cash/(context.idr_losers.size + 1))
                                   
    #Check Portfolio
    #log.info(type(context.portfolio.positions))
    record(leverage = context.account.leverage) #be sure to always track leverage
    record(cash = context.portfolio.cash)
    record(port_value = context.portfolio.portfolio_value)
   
def morning_day_trade1(context, data):
    context.day_count += 1
    log.info(context.daily_message, context.day_count)
    for stock in context.portfolio.positions:
        if((data.current(stock, 'price')) - context.portfolio.positions[stock].cost_basis)/context.portfolio.positions[stock].cost_basis > 0.001:
            if (context.portfolio.positions[stock].cost_basis > 0):
                num_shares = context.portfolio.positions[stock].amount
                order_target(stock, num_shares/2)
                log.info("{} Current Price = {} :: Cost Basis = {}",stock.symbol, data.current(stock, 'price'), context.portfolio.positions[stock].cost_basis)
                       
def morning_day_trade2(context, data):
    for stock in context.portfolio.positions:
        if((data.current(stock, 'price')) - context.portfolio.positions[stock].cost_basis)/context.portfolio.positions[stock].cost_basis > 0.001:
            if (context.portfolio.positions[stock].amount > 0): #or context.portfolio.positions[stock].amount < 0):
                order_target_percent(stock, 0)
                log.info("{} Current Price = {} :: Cost Basis = {}",stock.symbol, data.current(stock, 'price'), context.portfolio.positions[stock].cost_basis)
                
def morning_day_trade3(context, data):
    for stock in context.portfolio.positions:
        if((data.current(stock, 'price')) - context.portfolio.positions[stock].cost_basis)/context.portfolio.positions[stock].cost_basis > 0.2:
            if (context.portfolio.positions[stock].amount > 0): #or context.portfolio.positions[stock].amount < 0):
                order_target_percent(stock, 0)
                log.info("{} Current Price = {} :: Cost Basis = {}",stock.symbol, data.current(stock, 'price'), context.portfolio.positions[stock].cost_basis)
                
def check_portfolio(context, data): #Check for possible splits
    i = 0
    for stock in context.portfolio.positions:
        i = i + 1
        if ((data.current(stock, 'price') - context.portfolio.positions[stock].cost_basis)/(context.portfolio.positions[stock].cost_basis) > 1):
            log.info("{} Current Price = {} :: Cost Basis = {}",stock.symbol, data.current(stock, 'price'), context.portfolio.positions[stock].cost_basis)
    log.info("Portfolio Size: {}", i)
