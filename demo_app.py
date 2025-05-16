import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import pymongo
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go
from config import MONGO_URI, DB_NAME, COLLECTION_NAME

client = pymongo.MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Initialize Dash app
app = dash.Dash(__name__)

# Layout
app.layout = html.Div([
    html.H1('TranscribeMe KPIs Dashboard', style={'textAlign': 'center'}),
    html.Div([
        html.Label('Select Date Range:'),
        dcc.DatePickerRange(
            id='date-picker',
            min_date_allowed='2023-01-01',
            max_date_allowed=datetime.now().strftime('%Y-%m-%d'),
            start_date='2024-01-01',
            end_date='2025-05-06'
        ),
    ], style={'margin': '20px', 'textAlign': 'center'}),
    dcc.Graph(id='dau-graph'),
    dcc.Graph(id='mau-graph'),
    dcc.Graph(id='ndu-graph'),
    dcc.Graph(id='nmu-graph'),
    dcc.Graph(id='interactions-day-graph'),
    dcc.Graph(id='interactions-month-graph'),
], style={'padding': '10px'})

# Helper functions
def parse_dates(start_date, end_date):
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        if start > end:
            raise ValueError("Start date must be before end date.")
        return start, end
    except ValueError as e:
        print(f"Date parsing error: {e}")
        return None, None

def generate_monthly_dates(start_date, end_date):
    dates = []
    current_date = start_date.replace(day=1)
    end_date = end_date.replace(day=1)
    while current_date <= end_date:
        dates.append(current_date.strftime('%Y-%m-%d'))
        current_date += relativedelta(months=1)
    return dates

# DAU
def calculate_dau(start_date, end_date):
    pipeline = [
        {'$match': {'dt': {'$gte': start_date, '$lte': end_date}}},
        {'$group': {'_id': '$dt', 'unique_users': {'$addToSet': '$user_id'}}},
        {'$project': {'date': '$_id', 'user_count': {'$size': '$unique_users'}, '_id': 0}},
        {'$sort': {'date': 1}}
    ]
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['date'])
    all_days = pd.DataFrame({'date': pd.date_range(start=start_date, end=end_date, freq='D')})
    df = all_days.merge(df, on='date', how='left').fillna({'user_count': 0})
    return df

# MAU
def calculate_mau(start_date, end_date):
    pipeline = [
        {'$match': {'dt': {'$gte': start_date, '$lte': end_date}}},
        {
            '$group': {
                '_id': {'$dateToString': {'format': '%Y-%m', 'date': {'$toDate': '$dt'}}},
                'unique_users': {'$addToSet': '$user_id'}
            }
        },
        {'$project': {'month': '$_id', 'user_count': {'$size': '$unique_users'}, '_id': 0}},
        {'$sort': {'month': 1}}
    ]
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    all_months = pd.DataFrame({
        'month': [m.strftime('%Y-%m') for m in pd.period_range(start=start_date, end=end_date, freq='M')]
    })
    df = all_months.merge(df, on='month', how='left').fillna({'user_count': 0})
    return df

# NDU
def calculate_ndu(start_date, end_date):
    pipeline = [
        {'$match': {'dt': {'$gte': start_date, '$lte': end_date}}},
        {'$group': {'_id': '$user_id', 'first_activity': {'$min': '$dt'}}},
        {'$group': {'_id': '$first_activity', 'new_users_count': {'$sum': 1}}},
        {'$project': {'date': '$_id', 'new_users_count': 1, '_id': 0}},
        {'$sort': {'date': 1}}
    ]
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['date'])
    all_days = pd.DataFrame({'date': pd.date_range(start=start_date, end=end_date, freq='D')})
    df = all_days.merge(df, on='date', how='left').fillna({'new_users_count': 0})
    df['cumulative_new_users'] = df['new_users_count'].cumsum()
    return df

# NMU
def calculate_nmu(start_date, end_date):
    pipeline = [
        {'$match': {'dt': {'$gte': start_date, '$lte': end_date}}},
        {'$group': {'_id': '$user_id', 'first_activity': {'$min': '$dt'}}},
        {
            '$project': {
                'year': {'$substr': ['$first_activity', 0, 4]},
                'month': {'$substr': ['$first_activity', 5, 2]}
            }
        },
        {'$group': {'_id': {'year': '$year', 'month': '$month'}, 'new_users_count': {'$sum': 1}}},
        {'$project': {'year_month': {'$concat': ['$_id.year', '-', '$_id.month']}, 'new_users_count': 1, '_id': 0}},
        {'$sort': {'year_month': 1}}
    ]
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['year_month'] + '-01')
    df['month_name'] = df['date'].dt.strftime('%b %Y')
    df['cumulative_new_users'] = df['new_users_count'].cumsum()
    return df

# Interactions by Day
def calculate_interactions_day(start_date, end_date):
    pipeline = [
        {'$match': {'dt': {'$gte': start_date, '$lte': end_date}}},
        {
            '$group': {
                '_id': '$dt',
                'text_count': {'$sum': '$interactions'},
                'audio_count': {'$sum': '$audios'},
                'chat_count': {'$sum': '$chats'}
            }
        },
        {'$project': {'date': '$_id', 'text_count': 1, 'audio_count': 1, 'chat_count': 1, '_id': 0}},
        {'$sort': {'date': 1}}
    ]
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['date'])
    all_days = pd.DataFrame({'date': pd.date_range(start=start_date, end=end_date, freq='D')})
    df = all_days.merge(df, on='date', how='left').fillna({'text_count': 0, 'audio_count': 0, 'chat_count': 0})
    return df

# Interactions by Month
def calculate_interactions_month(start_date, end_date):
    pipeline = [
        {'$match': {'dt': {'$gte': start_date, '$lte': end_date}}},
        {
            '$group': {
                '_id': {'$dateToString': {'format': '%Y-%m', 'date': {'$toDate': '$dt'}}},
                'text_count': {'$sum': '$interactions'},
                'audio_count': {'$sum': '$audios'},
                'chat_count': {'$sum': '$chats'}
            }
        },
        {'$project': {'month': '$_id', 'text_count': 1, 'audio_count': 1, 'chat_count': 1, '_id': 0}},
        {'$sort': {'month': 1}}
    ]
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()
    df = pd.DataFrame(results)
    all_months = pd.DataFrame({
        'month': [m.strftime('%Y-%m') for m in pd.period_range(start=start_date, end=end_date, freq='M')]
    })
    df = all_months.merge(df, on='month', how='left').fillna({'text_count': 0, 'audio_count': 0, 'chat_count': 0})
    df['date'] = pd.to_datetime(df['month'] + '-01')
    df['month_name'] = df['date'].dt.strftime('%b %Y')
    return df

# Callback to update graphs
@app.callback(
    [
        Output('dau-graph', 'figure'),
        Output('mau-graph', 'figure'),
        Output('ndu-graph', 'figure'),
        Output('nmu-graph', 'figure'),
        Output('interactions-day-graph', 'figure'),
        Output('interactions-month-graph', 'figure')
    ],
    [Input('date-picker', 'start_date'), Input('date-picker', 'end_date')]
)
def update_graphs(start_date, end_date):
    start, end = parse_dates(start_date, end_date)
    if not start or not end:
        return [go.Figure() for _ in range(6)]

    # Calculate metrics
    dau_df = calculate_dau(start_date, end_date)
    mau_df = calculate_mau(start_date, end_date)
    ndu_df = calculate_ndu(start_date, end_date)
    nmu_df = calculate_nmu(start_date, end_date)
    interactions_day_df = calculate_interactions_day(start_date, end_date)
    interactions_month_df = calculate_interactions_month(start_date, end_date)

    # DAU Plot
    dau_fig = px.line(dau_df, x='date', y='user_count', title=f'DAU ({start_date} to {end_date})')
    dau_fig.update_layout(xaxis_title='Date', yaxis_title='Unique Users', xaxis_tickangle=45)

    # MAU Plot
    mau_fig = px.line(mau_df, x='month', y='user_count', title=f'MAU ({start_date} to {end_date})', markers=True)
    mau_fig.update_layout(xaxis_title='Month', yaxis_title='Unique Users', xaxis_tickangle=45)

    # NDU Plot
    ndu_fig = go.Figure()
    ndu_fig.add_trace(go.Bar(x=ndu_df['date'], y=ndu_df['new_users_count'], name='Daily New Users'))
    ndu_fig.add_trace(go.Scatter(x=ndu_df['date'], y=ndu_df['cumulative_new_users'], name='Cumulative New Users', yaxis='y2'))
    ndu_fig.update_layout(
        title=f'New Daily Users ({start_date} to {end_date})',
        xaxis_title='Date',
        yaxis_title='New Users',
        yaxis2={'title': 'Cumulative Users', 'overlaying': 'y', 'side': 'right'},
        xaxis_tickangle=45,
        legend=dict(x=0, y=1.1, orientation='h')
    )

    # NMU Plot
    nmu_fig = go.Figure()
    nmu_fig.add_trace(go.Bar(x=nmu_df['month_name'], y=nmu_df['new_users_count'], name='Monthly New Users'))
    nmu_fig.add_trace(go.Scatter(x=nmu_df['month_name'], y=nmu_df['cumulative_new_users'], name='Cumulative New Users', yaxis='y2'))
    nmu_fig.update_layout(
        title=f'New Monthly Users ({start_date} to {end_date})',
        xaxis_title='Month',
        yaxis_title='New Users',
        yaxis2={'title': 'Cumulative Users', 'overlaying': 'y', 'side': 'right'},
        xaxis_tickangle=45,
        legend=dict(x=0, y=1.1, orientation='h')
    )

    # Interactions by Day Plot
    interactions_day_fig = go.Figure()
    interactions_day_fig.add_trace(go.Scatter(x=interactions_day_df['date'], y=interactions_day_df['text_count'], name='Total Interactions'))
    interactions_day_fig.add_trace(go.Scatter(x=interactions_day_df['date'], y=interactions_day_df['audio_count'], name='Audio Interactions'))
    interactions_day_fig.add_trace(go.Scatter(x=interactions_day_df['date'], y=interactions_day_df['chat_count'], name='Chat Interactions'))
    interactions_day_fig.update_layout(
        title=f'Interactions by Day ({start_date} to {end_date})',
        xaxis_title='Date',
        yaxis_title='Interactions',
        xaxis_tickangle=45,
        legend=dict(x=0, y=1.1, orientation='h')
    )

    # Interactions by Month Plot
    interactions_month_fig = go.Figure()
    interactions_month_fig.add_trace(go.Scatter(x=interactions_month_df['month_name'], y=interactions_month_df['text_count'], name='Total Interactions', mode='lines+markers'))
    interactions_month_fig.add_trace(go.Scatter(x=interactions_month_df['month_name'], y=interactions_month_df['audio_count'], name='Audio Interactions', mode='lines+markers'))
    interactions_month_fig.add_trace(go.Scatter(x=interactions_month_df['month_name'], y=interactions_month_df['chat_count'], name='Chat Interactions', mode='lines+markers'))
    interactions_month_fig.update_layout(
        title=f'Interactions by Month ({start_date} to {end_date})',
        xaxis_title='Month',
        yaxis_title='Interactions',
        xaxis_tickangle=45,
        legend=dict(x=0, y=1.1, orientation='h')
    )

    return dau_fig, mau_fig, ndu_fig, nmu_fig, interactions_day_fig, interactions_month_fig

server = app.server  # para que Gunicorn pueda encontrarlo
# Run the app
if __name__ == '__main__':
    app.run(debug=True)
