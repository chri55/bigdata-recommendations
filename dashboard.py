import dash
import dash_core_components as dcc
import plotly.graph_objs as go
import dash_html_components as html
import dash_table
import pandas as pd
import numpy as np

def draw_dashboard(app):
    df = pd.read_json('json/averageRating.json', lines=True)
    df = df.dropna(how='any', axis=0)
    #display(df)
    sample = df.sample(n = 10000)
    #display(sample)
    new_df = sample.rename(columns = {"asin": "Product ID", "title": "Title", "average_rating":"Average Rating", "price": "Price"})
    sorted_average = new_df.sort_values(by='Average Rating', ascending=0)
    #display(sorted_average)

    sorted_average = new_df.sort_values(by='Average Rating', ascending=0)
    #display(sorted_average)
    top_average = sorted_average.head(10)
    #display(top_average)
    bottom_average = sorted_average.tail(10)
    #display(bottom_average)

    app.layout = html.Div([
        dcc.Graph(
            id='scatter_chart',
            figure = {
                'data' : [
                    go.Scatter(
                        x = sample.average_rating,
                        y = sample.price,
                        mode = 'markers',
                        hovertemplate = sample.title
                    )
                ],
                'layout' : go.Layout(
                    title = "Scatterplot of Average Rating of 10000 Random Books",
                    xaxis = {'title' : 'Average Rating'},
                    yaxis = {'title' : 'Price'},
                    hovermode = 'closest',
                )
            }
        ),
        html.H3('Top 10 Most Favorite Books'),
        dash_table.DataTable(
            id='topaverage_table',
            columns = [{"name": i, "id": i} for i in top_average.columns],
            data = top_average.to_dict("rows"),
            style_cell_conditional=[
                {
                    'if': {'column_id': c},
                    'textAlign': 'left'
                } for c in ['Product ID', 'Title', 'Average Rating', 'Price']
            ],
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(248, 248, 248)'
                }
            ],
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold'
            }
        ),
        html.H3('Top 10 Least Favorite Books'),
        dash_table.DataTable(
            id='bottomaverage_table',
            columns = [{"name": i, "id": i} for i in bottom_average.columns],
            data = bottom_average.to_dict("rows"),
            style_cell_conditional=[
                {
                    'if': {'column_id': c},
                    'textAlign': 'left'
                } for c in ['Product ID', 'Title', 'Average Rating', 'Price']
            ],
            style_data_conditional=[
                {
                    'if': {'row_index': 'odd'},
                    'backgroundColor': 'rgb(248, 248, 248)'
                }
            ],
            style_header={
                'backgroundColor': 'rgb(230, 230, 230)',
                'fontWeight': 'bold'
            }
        )
        
    ])