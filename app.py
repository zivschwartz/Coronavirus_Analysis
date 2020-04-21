import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objects as go
import plotly.express as px
from dash.dependencies import Input, Output
import pandas as pd
import numpy as np

company_data_monthly = pd.read_csv('company_financial_data_41.csv')
company_df = pd.read_csv('company_for_priceindex.xls')

def generate_table(dataframe):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(len(dataframe))
        ])
    ])

financial_indicators = company_data_monthly.sort_values('yyyy-mm-dd')
for comp in company_data_monthly.columns.tolist()[1:]:
    financial_indicators[comp + '_change'] = (financial_indicators[comp] - financial_indicators[comp].shift(1))/financial_indicators[comp].shift(1)

financial_indicators_DJI = financial_indicators[['yyyy-mm-dd']]
for comp in [i for i in financial_indicators.columns if '_change' in i]:
    financial_indicators_DJI[comp] = financial_indicators[comp]/financial_indicators['DJI_change']

def normalize(x_pre, x):
    min_val = min(x_pre)
    max_val = max(x_pre)
    
    r = max_val - min_val    
    x_transformed = [(i - min_val)/r for i in x]
    return x_transformed

comp_notnull = company_data_monthly.loc[:, company_data_monthly.isnull().sum() < company_data_monthly.shape[0]].columns[1:]


company_data_daily_normalized = pd.DataFrame()
company_data_daily_normalized['yyyy-mm-dd'] = company_data_monthly['yyyy-mm-dd']
for company in comp_notnull:
    x = list(company_data_monthly[company].values)
    x_pre = np.array(company_data_monthly.loc[company_data_monthly['yyyy-mm-dd'] < '2020-02-01', company].values)

    company_data_daily_normalized[company] = normalize(x_pre, x)


comp_notnull = company_data_monthly.loc[:, company_data_monthly.isnull().sum() < company_data_monthly.shape[0]].columns[1:]

company_data_monthly['yyyy-mm-dd'] = pd.to_datetime(company_data_monthly['yyyy-mm-dd'])
for c in comp_notnull:
    company_data_monthly[c] = company_data_monthly[c].astype(float)


company_data_daily_normalized_stacked = pd.DataFrame(columns=['yyyy-mm-dd','normalized_stock_price', 'company_index'])
for c in comp_notnull:
    d = company_data_daily_normalized.loc[:,['yyyy-mm-dd']+[c]]
    d.columns = ['yyyy-mm-dd','normalized_stock_price']
    d['company_index'] = c
    company_data_daily_normalized_stacked = company_data_daily_normalized_stacked.append(d)
company_data_daily_normalized_stacked = company_data_daily_normalized_stacked.merge(company_df, 
                                                                                    on='company_index',
                                                                                    how='left')

                                   
# fig = px.line(company_data_daily_normalized_stacked, x="yyyy-mm-dd", y='normalized_stock_price', 
#               line_group='company_index', color='hypothesis',
#               hover_name="company")
# Create figure
fig = go.Figure()

for c in company_data_daily_normalized_stacked.company_index.unique():
    df = company_data_daily_normalized_stacked.loc[company_data_daily_normalized_stacked.company_index==c]
    x = df["yyyy-mm-dd"]
    y= df['normalized_stock_price']
    
    fig.add_trace(go.Scatter(
    x=x,
    y=y,
#     fill='toself',
#     fillcolor='rgba(0,100,80,0.2)',
#     line_color='rgba(255,255,255,0)',
#     showlegend=False,
    name=company_df.loc[company_df.company_index==c,'company'].values[0]
    ))

# Set title
fig.update_layout(
    title_text="Time series with range slider and selectors"
)

# Add range slider
fig.update_layout(
    yaxis=dict(
       autorange = True,
       fixedrange= False
   ),
    xaxis=dict(
        rangeselector=dict(
            buttons=list([
                dict(count=1,
                     label="1m",
                     step="month",
                     stepmode="backward"),
                dict(count=6,
                     label="6m",
                     step="month",
                     stepmode="backward"),
                dict(count=1,
                     label="YTD",
                     step="year",
                     stepmode="todate"),
                dict(count=1,
                     label="1y",
                     step="year",
                     stepmode="backward"),
                dict(step="all")
            ])
        ),
        rangeslider=dict(
            visible=True
        ),
        type="date"
    )
)

#fig.show()


### BUILD DASHBOARD

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(
    	children='COVID-19',
    	style={
    		'textAlign': 'center'
    	}
    ),

    html.H4(
    	children='Analyzing the spread and effect of the coronavirus',
    	style={
    		'textAlign': 'center'
    	}
    ),

    dcc.Graph(
        id='example-graph',
        figure = fig
    ),

    html.Div(
    	children=[
    		html.H4(
    			children='Company Data Normalized Daily',
    			style={
    				'textAlign': 'center'
    			}),
    		generate_table(company_data_daily_normalized_stacked)
	])
])

    

if __name__ == '__main__':
    app.run_server(debug=True)
