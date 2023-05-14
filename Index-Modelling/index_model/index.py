import datetime as dt
import pandas as pd
import numpy as np
# import pyfolio as pyf
# import matplotlib.pyplot as plt
# import seaborn as sb
# %matplotlib inline
# pd.set_option('display.max_columns', None)

path_to_data='../data_sources/stock_prices.csv'


class IndexModel():
    def __init__(self) -> None:
        stock_ret=pd.read_csv(path_to_data)
        
        stock_ret['Date'] = pd.to_datetime(stock_ret['Date'],dayfirst=True)

        ### as index business days are Monday to Friday
        stock_ret['day_of_week'] = stock_ret['Date'].dt.day_name()
        # stock_ret['month'] = stock_ret['Date'].dt.month
        stock_ret = stock_ret[~((stock_ret['day_of_week']=='Saturday')|(stock_ret['day_of_week']=='Sunday'))]
        stock_ret = stock_ret.drop('day_of_week', axis=1)


        
        index_df=self.calc_index_level(stock_ret,'2019-12-31', '2020-12-31')
        self.export_values(index_df[['Cal_Index']], "export.csv")
        
        
        

    def calc_index_level(self, stock_ret, start_date: dt.date, end_date: dt.date) -> None:
        
        stock_ret.set_index('Date', inplace=True)
        stock_ret=stock_ret[start_date:end_date]
        stock_ret.sort_index(inplace=True)
        # stock_ret=stock_ret[1:]
        
        stocks=stock_ret.columns.to_list()

        ###create weights df
        wt_df = pd.DataFrame(index=[stock_ret.index[0]])

        for s in stocks:
            wt_df[s + '_wt'] = stock_ret[s][0]
            
            
         # initialize variables
        balance_month = stock_ret.index[0].month
        signal = False # the signal whether it's a rebalancing day or not
        count = 0   



        prev_values = {}

        for day in stock_ret.index:
            count += 1
    
            if day == stock_ret.index[0]:
                wt_df.loc[day] = wt_df.loc[day] # First day

                # Store initial values as previous values
                for col in wt_df.columns:
                    prev_values[col] = wt_df.loc[day, col]


    
            elif day.month != balance_month:
                signal = True
                # calculate new weights based on the new portfolio value
                new_wts_org = [stock_ret[s].shift()[day] for s in stocks]

                # pick top three stocks 
                sorted_wts=sorted(new_wts_org)
                new_wt=[x if x>=sorted_wts[-3] else 0 for x in new_wts_org]

                # apply weights on top 3 stocks
                new_wt[new_wt.index(min(x for x in new_wt if x>0))]=0.25
                new_wt[new_wt.index(min(x for x in new_wt if x>1))]=0.25
                new_wt[new_wt.index(max(new_wt))]=0.5


                wt_df.loc[day, :] = new_wt
                balance_month = day.month
                count += 1
                # print(f'Rebalance: {day.date()}, count: {count}') # uncomment to debug days ;)
                # Store new values as previous values
                for col in  wt_df.columns:
                    prev_values[col] =  wt_df.loc[day, col]

            else:
                signal = False

                # Use previous values if it is not a rebalancing date
                wt_df.loc[day, :] = [prev_values[col] for col in wt_df.columns]

    

        
            # Calculate asset values and portfolio value for the current day
            asset_values = [wt_df.loc[day, s + '_wt'] * stock_ret.loc[day, s] for s in stocks]
            portfolio_value = sum(asset_values)
    
            stock_ret.loc[day, 'Signal'] = signal
            stock_ret.loc[day, 'Portfolio_Value'] = portfolio_value
    
            # Add weights & portfolio values to stock return data frame
            for s in stocks:
                stock_ret.loc[day, s + '_wt'] = wt_df.loc[day, s + '_wt']


        ###align index for different portfolios
        stock_ret=stock_ret[1:]

        stock_ret['Portfolio_Value_Index']=stock_ret['Stock_A']*stock_ret['Stock_A_wt'].shift()+stock_ret['Stock_B']*stock_ret['Stock_B_wt'].shift()\
            +stock_ret['Stock_C']*stock_ret['Stock_C_wt'].shift()+stock_ret['Stock_D']*stock_ret['Stock_D_wt'].shift()+stock_ret['Stock_E']*stock_ret['Stock_E_wt'].shift()\
                +stock_ret['Stock_F']*stock_ret['Stock_F_wt'].shift()+stock_ret['Stock_G']*stock_ret['Stock_G_wt'].shift()+stock_ret['Stock_H']*stock_ret['Stock_H_wt'].shift()\
                    +stock_ret['Stock_I']*stock_ret['Stock_I_wt'].shift()+stock_ret['Stock_J']*stock_ret['Stock_J_wt'].shift()

        ###set initial aggregations
        stock_ret['Portfolio_Value_Index'][0]=stock_ret['Portfolio_Value'][0]

        stock_ret.reset_index(inplace=True)
        stock_ret['month'] = stock_ret['Date'].dt.month

        stock_ret['First_Col_Index'] = stock_ret.groupby('month')['Portfolio_Value_Index'].transform('first')
        stock_ret['First_Col'] = stock_ret.groupby('month')['Portfolio_Value'].transform('first')
        stock_ret['First_Col_shift_']=stock_ret['First_Col'].shift()
        stock_ret['First_Col_shift'] = stock_ret.groupby('month')['First_Col_shift_'].transform('first')


        stock_ret['Cal_Index']=(stock_ret['Portfolio_Value']/stock_ret['First_Col'])*(stock_ret['First_Col_Index']/stock_ret['First_Col_shift'])*100
        stock_ret.set_index('Date', inplace=True)
        
        return stock_ret



    def export_values(self, df, file_name: str) -> None:
        df.to_csv(file_name)



IndexModel()