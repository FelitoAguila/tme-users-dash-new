import dash
from dash import dcc, html, Input, Output
import plotly.express as px
import pandas as pd
import pymongo
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import plotly.graph_objects as go
from get_country import getCountry

# MongoDB connection
MONGO_CONNECTION_STRING = "mongodb+srv://subs_dash_read_only:AxbX54tg64KGmEt9@transcribeme.rletx0y.mongodb.net/?retryWrites=true&w=majority&appName=Transcribeme"
client = pymongo.MongoClient(MONGO_CONNECTION_STRING)
db = client['Analytics']
collection = db['dau']

# Initialize Dash app
app = dash.Dash(__name__)

# Layout
app.layout = html.Div([
    html.H1('Dashboard de Usuarios - TranscribeMe', style={'textAlign': 'center'}),
    html.Div([
        html.Label('Select Date Range:'),
        dcc.DatePickerRange(
            id='date-picker',
            min_date_allowed='2023-01-01',
            max_date_allowed=datetime.now().strftime('%Y-%m-%d'),
            start_date='2025-01-01',
            end_date='2025-05-16'
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

def asign_countries(df):
    """
    Asigna el país a cada fila del DataFrame y devuelve el DataFrame actualizado.

    Parámetros:
    df (pd.DataFrame): DataFrame con columnas 'source' y 'user_id'

    Retorna:
    pd.DataFrame: DataFrame con la nueva columna 'country'
    """
    def determine_country(row):
        if row['source'] == 't':
            return "Telegram"
        else:
            user_phone = '+' + str(row['user_id'])
            return getCountry(user_phone)

    df['country'] = df.apply(determine_country, axis=1)
    return df

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

def calculate_dau_by_country(start_date, end_date):
    """
    Calcula usuarios activos diarios (DAU) entre dos fechas y los clasifica por país.
    
    Parámetros:
    start_date: Fecha de inicio (inclusive)
    end_date: Fecha de fin (inclusive)
    
    Retorna:
    DataFrame con conteo de usuarios por día y país
    """
    # Pipeline para obtener usuarios únicos por día, incluyendo source y user_id
    pipeline = [
        {'$match': {'dt': {'$gte': start_date, '$lte': end_date}}},
        {'$group': {
            '_id': {'date': '$dt', 'user_id': '$user_id', 'source': '$source'},
            'count': {'$sum': 1}
        }},
        {'$project': {
            'date': '$_id.date', 
            'user_id': '$_id.user_id', 
            'source': '$_id.source', 
            '_id': 0
        }},
        {'$sort': {'date': 1}}
    ]
    
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()
    
    # Crear DataFrame con los resultados
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['date'])
    
    # Asignar país a cada usuario
    df = asign_countries(df)
    
    # Agrupar por fecha y país para contar usuarios únicos
    country_dau = df.groupby(['date', 'country']).size().reset_index(name='user_count')
    
    # Asegurar que todas las combinaciones de días y países estén presentes
    all_days = pd.date_range(start=start_date, end=end_date, freq='D')
    unique_countries = df['country'].unique()
    
    # Crear una cuadrícula completa de fechas y países
    index = pd.MultiIndex.from_product([all_days, unique_countries], names=['date', 'country'])
    full_grid = pd.DataFrame(index=index).reset_index()
    
    # Combinar con datos reales
    final_df = full_grid.merge(country_dau, on=['date', 'country'], how='left').fillna({'user_count': 0})
    
    return final_df

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

def calculate_mau_by_country(start_date, end_date):
    """
    Calcula usuarios activos mensuales (MAU) entre dos fechas y los clasifica por país.
    
    Parámetros:
    start_date: Fecha de inicio (inclusive)
    end_date: Fecha de fin (inclusive)
    
    Retorna:
    DataFrame con conteo de usuarios únicos por mes y país
    """
    # Pipeline para obtener usuarios únicos por mes, incluyendo source y user_id
    pipeline = [
        {'$match': {'dt': {'$gte': start_date, '$lte': end_date}}},
        {
            '$group': {
                '_id': {
                    'month': {'$dateToString': {'format': '%Y-%m', 'date': {'$toDate': '$dt'}}},
                    'user_id': '$user_id',
                    'source': '$source'
                },
                'count': {'$sum': 1}
            }
        },
        {'$project': {
            'month': '$_id.month', 
            'user_id': '$_id.user_id', 
            'source': '$_id.source', 
            '_id': 0
        }},
        {'$sort': {'month': 1}}
    ]
    
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()
    
    # Crear DataFrame con los resultados
    df = pd.DataFrame(results)
    
    # Asignar país a cada usuario
    df = asign_countries(df)
    
    # Agrupar por mes y país para contar usuarios únicos
    country_mau = df.groupby(['month', 'country']).size().reset_index(name='user_count')
    
    # Asegurar que todas las combinaciones de meses y países estén presentes
    all_months = pd.DataFrame({
        'month': [m.strftime('%Y-%m') for m in pd.period_range(start=start_date, end=end_date, freq='M')]
    })
    unique_countries = df['country'].unique()
    
    # Crear una cuadrícula completa de meses y países
    month_country_grid = []
    for month in all_months['month']:
        for country in unique_countries:
            month_country_grid.append({'month': month, 'country': country})
    
    full_grid = pd.DataFrame(month_country_grid)
    
    # Combinar con datos reales
    final_df = full_grid.merge(country_mau, on=['month', 'country'], how='left').fillna({'user_count': 0})
    
    return final_df

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

def calculate_ndu_by_country(start_date, end_date):
    """
    Calcula nuevos usuarios diarios (NDU) entre dos fechas y los clasifica por país.
    
    Parámetros:
    start_date: Fecha de inicio (inclusive)
    end_date: Fecha de fin (inclusive)
    
    Retorna:
    DataFrame con conteo de nuevos usuarios por día y país, y el conteo acumulado
    """
    # Pipeline para obtener la primera actividad de cada usuario, incluyendo source
    pipeline = [
        {'$match': {'dt': {'$gte': start_date, '$lte': end_date}}},
        {
            '$group': {
                '_id': '$user_id', 
                'first_activity': {'$min': '$dt'},
                'source': {'$first': '$source'}  # Tomamos la primera fuente asociada al usuario
            }
        },
        {
            '$project': {
                'user_id': '$_id',
                'first_activity': 1,
                'source': 1,
                '_id': 0
            }
        },
        {'$sort': {'first_activity': 1}}
    ]
    
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()
    
    # Crear DataFrame con los resultados
    df = pd.DataFrame(results)
    df['date'] = pd.to_datetime(df['first_activity'])
    
    # Asignar país a cada usuario
    df = asign_countries(df)
    
    # Agrupar por fecha y país para contar nuevos usuarios
    country_ndu = df.groupby(['date', 'country']).size().reset_index(name='new_users_count')
    
    # Asegurar que todas las combinaciones de días y países estén presentes
    all_days = pd.date_range(start=start_date, end=end_date, freq='D')
    unique_countries = df['country'].unique()
    
    # Crear una cuadrícula completa de fechas y países
    day_country_grid = []
    for day in all_days:
        for country in unique_countries:
            day_country_grid.append({'date': day, 'country': country})
    
    full_grid = pd.DataFrame(day_country_grid)
    
    # Combinar con datos reales
    final_df = full_grid.merge(country_ndu, on=['date', 'country'], how='left').fillna({'new_users_count': 0})
    
    # Calcular el acumulado de nuevos usuarios por país
    final_df = final_df.sort_values(['country', 'date'])
    final_df['cumulative_new_users'] = final_df.groupby('country')['new_users_count'].cumsum()
    
    return final_df

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

def calculate_nmu_by_country(start_date, end_date):
    """
    Calcula nuevos usuarios mensuales (NMU) entre dos fechas y los clasifica por país.
    
    Parámetros:
    start_date: Fecha de inicio (inclusive)
    end_date: Fecha de fin (inclusive)
    
    Retorna:
    DataFrame con conteo de nuevos usuarios por mes y país, y el conteo acumulado
    """
    # Pipeline para obtener la primera actividad de cada usuario, incluyendo source
    pipeline = [
        {'$match': {'dt': {'$gte': start_date, '$lte': end_date}}},
        {
            '$group': {
                '_id': '$user_id', 
                'first_activity': {'$min': '$dt'},
                'source': {'$first': '$source'}  # Tomamos la primera fuente asociada al usuario
            }
        },
        {
            '$project': {
                'user_id': '$_id',
                'first_activity': 1,
                'source': 1,
                'year': {'$substr': ['$first_activity', 0, 4]},
                'month': {'$substr': ['$first_activity', 5, 2]},
                '_id': 0
            }
        },
        {'$sort': {'first_activity': 1}}
    ]
    
    results = list(collection.aggregate(pipeline))
    if not results:
        return pd.DataFrame()
    
    # Crear DataFrame con los resultados
    df = pd.DataFrame(results)
    
    # Crear columna year_month para agrupar por mes
    df['year_month'] = df['year'] + '-' + df['month']
    
    # Asignar país a cada usuario
    df = asign_countries(df)
    
    # Agrupar por mes y país para contar nuevos usuarios
    country_nmu = df.groupby(['year_month', 'country']).size().reset_index(name='new_users_count')
    
    # Asegurar que todas las combinaciones de meses y países estén presentes
    # Obtener todos los meses en el rango de fechas
    all_months = pd.DataFrame({
        'year_month': [m.strftime('%Y-%m') for m in pd.period_range(start=start_date, end=end_date, freq='M')]
    })
    unique_countries = df['country'].unique()
    
    # Crear una cuadrícula completa de meses y países
    month_country_grid = []
    for month in all_months['year_month']:
        for country in unique_countries:
            month_country_grid.append({'year_month': month, 'country': country})
    
    full_grid = pd.DataFrame(month_country_grid)
    
    # Combinar con datos reales
    final_df = full_grid.merge(country_nmu, on=['year_month', 'country'], how='left').fillna({'new_users_count': 0})
    
    # Agregar fecha y nombre del mes para mejor visualización
    final_df['date'] = pd.to_datetime(final_df['year_month'] + '-01')
    final_df['month_name'] = final_df['date'].dt.strftime('%b %Y')
    
    # Calcular el acumulado de nuevos usuarios por país
    final_df = final_df.sort_values(['country', 'date'])
    final_df['cumulative_new_users'] = final_df.groupby('country')['new_users_count'].cumsum()
    
    return final_df

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
    #dau_df = calculate_dau(start_date, end_date)
    #mau_df = calculate_mau(start_date, end_date)
    #ndu_df = calculate_ndu(start_date, end_date)
    #nmu_df = calculate_nmu(start_date, end_date)
    dau_df_by_country = calculate_dau_by_country(start_date, end_date)
    mau_df_by_country = calculate_mau_by_country(start_date, end_date)
    ndu_df_by_country = calculate_ndu_by_country(start_date, end_date)
    nmu_df_by_country = calculate_nmu_by_country(start_date, end_date)
    interactions_day_df = calculate_interactions_day(start_date, end_date)
    interactions_month_df = calculate_interactions_month(start_date, end_date)

    # DAU Plot
    #dau_fig = px.line(dau_df, x='date', y='user_count', title=f'DAU ({start_date} to {end_date})')
    #dau_fig.update_layout(xaxis_title='Date', yaxis_title='Unique Users', xaxis_tickangle=45)

    dau_fig = px.bar(dau_df_by_country, x='date', y='user_count', color='country', 
                     title=f'DAU ({start_date} to {end_date})', barmode='stack')
    dau_fig.update_layout(xaxis_title='Fecha', yaxis_title='Usuarios Únicos', xaxis_tickangle=45,legend_title='País')

    # MAU Plot
    mau_fig = px.bar(mau_df_by_country, x='month', y='user_count', color='country', 
                      title=f'MAU ({start_date} to {end_date})', barmode='stack')
    mau_fig.update_layout(xaxis_title='Fecha', yaxis_title='Usuarios Únicos', xaxis_tickangle=45,legend_title='País')

    # NDU Plot
    ndu_fig = px.bar(ndu_df_by_country, x='date', y='new_users_count', color='country', 
                 title=f'New Daily Users ({start_date} to {end_date})', barmode='stack')
    ndu_fig.update_layout(xaxis_title='Fecha', yaxis_title='Usuarios Únicos', xaxis_tickangle=45,legend_title='País')

    # NMU Plot
    nmu_fig = px.bar(nmu_df_by_country, x='month_name', y='new_users_count', color='country', 
                 title=f'New Monthly Users ({start_date} to {end_date})', barmode='stack')
    nmu_fig.update_layout(xaxis_title='Fecha', yaxis_title='Usuarios Únicos', xaxis_tickangle=45,legend_title='País')

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
