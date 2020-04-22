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

                                   
hypothesis_colors = {'increase':'green', 'decrease':'red'}

colors = ['red', 'orange', 'yellow', 'green', 'powderblue', 'blue', 'magenta', 'purple', 'black']

industry_colors = {company_df.group.unique()[i]: colors[i] for i in range(len(company_df.group.unique()))}

# Create figure
company_data_daily_normalized_stacked = company_data_daily_normalized_stacked.loc[company_data_daily_normalized_stacked['yyyy-mm-dd'] < '2020-04-01']

fig = go.Figure()
industry_plots = []
hypothesis_plots = []

for c in company_data_daily_normalized_stacked.company_index.unique():
    df = company_data_daily_normalized_stacked.loc[company_data_daily_normalized_stacked.company_index==c]
    x = df["yyyy-mm-dd"]
    y= df['normalized_stock_price']
    industry = company_df.loc[company_df.company_index==c,'group'].values[0]
    hypothesis = company_df.loc[company_df.company_index==c,'hypothesis'].values[0]
    industry_plots.append(industry)
   
    fig.add_trace(go.Scatter(
    x=x,
    y=y,
    name=company_df.loc[company_df.company_index==c,'company'].values[0],
    line=dict(color=industry_colors[industry]),
    mode='lines',
    hovertemplate=
        ('%s (%s) <br><br>Industry: %s <br>Hypothesis: %s' %(company_df.loc[company_df.company_index==c, 'company'].values[0], c, industry, hypothesis))
                 ))


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

industry_layout = [dict(label = 'All',
                  method = 'update',
                  args = [{'visible': [True for i in range(len(industry_plots))]},
                          {'title': 'All',
                           'showlegend':True}])]
for ind in company_df.group.unique():
    ind_dict = dict(label = ind, method = 'update',
      args = [{'visible': [True if ind == i else False for i in industry_plots]}, # the index of True aligns with the indices of plot traces
              {
               'showlegend':True}])
    industry_layout.append(ind_dict)
    
    
fig.update_layout(
    updatemenus=[go.layout.Updatemenu(
        active=0,
        buttons=list(industry_layout)
        )
    ])


fig.update_layout(
    hoverlabel=dict(
        bgcolor="white",
        font_size=16,
        font_family="Rockwell"
    ))

fig.update_layout(title='Normalized Stock Index')
#fig.show()

company_data_daily_normalized_stacked = company_data_daily_normalized_stacked.loc[company_data_daily_normalized_stacked['yyyy-mm-dd'] < '2020-04-01']

fig1 = go.Figure()
industry_plots = []
hypothesis_plots = []

for c in company_data_daily_normalized_stacked.company_index.unique():
    df = company_data_daily_normalized_stacked.loc[company_data_daily_normalized_stacked.company_index==c]
    x = df["yyyy-mm-dd"]
    y= df['normalized_stock_price']
    industry = company_df.loc[company_df.company_index==c,'group'].values[0]
    industry_plots.append(industry)
    hypothesis = company_df.loc[company_df.company_index==c,'hypothesis'].values[0]
    hypothesis_plots.append(hypothesis)
   
    fig1.add_trace(go.Scatter(
    x=x,
    y=y,
    name=company_df.loc[company_df.company_index==c,'company'].values[0],
    line=dict(color=hypothesis_colors[hypothesis]),
    mode='lines',
    hovertemplate=
        ('%s (%s) <br><br>Industry: %s <br>Hypothesis: %s' %(company_df.loc[company_df.company_index==c, 'company'].values[0], c, industry, hypothesis))
                 ))


# Add range slider
fig1.update_layout(
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


hypothesis_layout = [dict(label = 'All',
                  method = 'update',
                  args = [{'visible': [True for i in range(len(hypothesis_plots))]},
                          {'title': 'All',
                           'showlegend':True}])]

for hyp in company_df.hypothesis.unique():
    hyp_dict = dict(label = hyp, method = 'update',
      args = [{'visible': [True if hyp == i else False for i in hypothesis_plots]}, # the index of True aligns with the indices of plot traces
              {
               'showlegend':True}])
    hypothesis_layout.append(hyp_dict)

fig1.update_layout(
    updatemenus=[go.layout.Updatemenu(
        active=0,
        buttons=list(hypothesis_layout)
        )
    ])

fig1.update_layout(title='Hypothesized Stock Index') 

# x_dates = list(x)
# x_dates.reverse()


### BUILD DASHBOARD ###

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

server = app.server

app.layout = html.Div(children=[
    html.H1(
    	children='COVID-19',
    	style={
    		'textAlign': 'center'
    	}
    ),

    html.H4(
    	children='Analyzing the spread and effect of the coronavirus across the world',
    	style={
    		'textAlign': 'center'
    	}
    ),

	# dcc.RangeSlider(
 #    	id='slider',
 #    	marks = {i : x_dates[i] for i in range(len(x_dates))},
 #    	min=0,
 #    	max=89,
 #    	value=[30,80]
	# ),

	dcc.Graph(
        id='industry-stock-graph',
        figure = fig
    ),

    dcc.Markdown('''
    	The above graph shows the normalized stock indices for different companies from around Thanksgiving
    	to the end of March.
    	'''),

    dcc.Graph(
        id='hypothesis-stock-graph',
        figure = fig1
    ),

    dcc.Markdown('''
    	The above graph shows the normalized stock indices for different companies from around Thanksgiving
    	to the end of March in which we hypothesized would either increase or decrease.
    	'''),
    
])

# @app.callback(
#     Output('industry-stock-graph','figure'), 
#     [Input('slider','value')]
# )

# def update_figure(input2):
#     for c in company_data_daily_normalized_stacked.company_index.unique():
#         df = company_data_daily_normalized_stacked.loc[company_data_daily_normalized_stacked.company_index==c]
#         x = df["yyyy-mm-dd"]
#         y= df['normalized_stock_price']
#         industry = company_df.loc[company_df.company_index==c,'group'].values[0]
#         hypothesis = company_df.loc[company_df.company_index==c,'hypothesis'].values[0]
#         industry_plots.append(industry)
        
#         x2 = x[(x > x_dates[input2[0]]) & (x < x_dates[input2[1]])]

#         fig.add_trace(go.Scatter(
#         x=x2,
#         y=y,
#         name=company_df.loc[company_df.company_index==c,'company'].values[0],
#         line=dict(color=industry_colors[industry]),
#         mode='lines',
#         hovertemplate=
#             ('%s (%s) <br><br>Industry: %s <br>Hypothesis: %s' %(company_df.loc[company_df.company_index==c, 'company'].values[0], c, industry, hypothesis))
#                  ))
#         # Add range slider

#     # Add range slider
#     fig.update_layout(
#         yaxis=dict(
#            autorange = True,
#            fixedrange= False
#        ),
#         xaxis=dict(
#             rangeselector=dict(
#                 buttons=list([
#                     dict(count=1,
#                          label="1m",
#                          step="month",
#                          stepmode="backward"),
#                     dict(count=6,
#                          label="6m",
#                          step="month",
#                          stepmode="backward"),
#                     dict(count=1,
#                          label="YTD",
#                          step="year",
#                          stepmode="todate"),
#                     dict(count=1,
#                          label="1y",
#                          step="year",
#                          stepmode="backward"),
#                     dict(step="all")
#                 ])
#             ),
#             rangeslider=dict(
#                 visible=True
#             ),
#             type="date"
#         )
#     )

#     industry_layout = [dict(label = 'All',
#                       method = 'update',
#                       args = [{'visible': [True for i in range(len(industry_plots))]},
#                               {'title': 'All',
#                                'showlegend':True}])]
#     for ind in company_df.group.unique():
#         ind_dict = dict(label = ind, method = 'update',
#           args = [{'visible': [True if ind == i else False for i in industry_plots]}, # the index of True aligns with the indices of plot traces
#                   {
#                    'showlegend':True}])
#         industry_layout.append(ind_dict)


#     fig.update_layout(
#         updatemenus=[go.layout.Updatemenu(
#             active=0,
#             buttons=list(industry_layout)
#             )
#         ])


#     fig.update_layout(
#         hoverlabel=dict(
#             bgcolor="white",
#             font_size=16,
#             font_family="Rockwell"
#         )
#     )


#     return fig
    

if __name__ == '__main__':
    app.run_server(debug=True)
