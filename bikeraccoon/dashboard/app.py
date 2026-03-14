import dash
from dash import dcc, html, Input, Output
import plotly.graph_objects as go
import pandas as pd
import datetime as dt
from urllib.parse import parse_qs

import bikeraccoon as br

app = dash.Dash(
    __name__,
    title='Bikeraccoon Dashboard',
    suppress_callback_exceptions=True,
)
server = app.server

COLORS = {
    'trips': '#2196F3',
    'returns': '#FF9800',
    'bg': '#f5f5f5',
    'card': '#ffffff',
    'text': '#333333',
    'muted': '#888888',
}

CARD_STYLE = {
    'backgroundColor': COLORS['card'],
    'borderRadius': '8px',
    'padding': '1.5rem',
    'boxShadow': '0 1px 4px rgba(0,0,0,0.1)',
}

app.layout = html.Div([
    dcc.Location(id='url', refresh=False),
    dcc.Store(id='current-system'),

    # Header
    html.Header([
        html.A('Bikeraccoon', href='/', style={
            'textDecoration': 'none',
            'color': '#333',
            'fontSize': '1.4rem',
            'fontWeight': '700',
        }),
        html.Div([
            html.A('API', href='https://api.raccoon.bike', target='_blank', style={
                'textDecoration': 'none',
                'color': COLORS['muted'],
                'fontSize': '0.95rem',
                'fontWeight': '500',
                'marginRight': '1.5rem',
            }),
            html.Div([
                html.Button('Bluesky ▾', className='nav-dropdown-btn'),
                html.Div([
                    html.Div([
                        html.A('Montreal', href='https://bsky.app/profile/montreal.raccoon.bike', target='_blank'),
                        html.A('Toronto', href='https://bsky.app/profile/toronto.raccoon.bike', target='_blank'),
                        html.A('Vancouver', href='https://bsky.app/profile/vancouver.raccoon.bike', target='_blank'),
                    ], className='nav-dropdown-menu-inner'),
                ], className='nav-dropdown-menu'),
            ], className='nav-dropdown'),
        ], style={'display': 'flex', 'alignItems': 'center'}),
    ], style={
        'padding': '1rem 2rem',
        'borderBottom': '1px solid #e0e0e0',
        'backgroundColor': 'white',
        'display': 'flex',
        'alignItems': 'center',
        'justifyContent': 'space-between',
        'position': 'sticky',
        'top': '0',
        'zIndex': '200',
    }),

    # Front page
    html.Div(id='frontpage', children=[
        html.H2('Tracked Bike Share Systems', style={
            'marginBottom': '1.5rem',
            'fontWeight': '600',
        }),
        html.Div(id='system-grid'),
    ], style={'padding': '2rem'}),

    # System page
    html.Div(id='systempage', style={'display': 'none', 'padding': '2rem'}, children=[
        html.A('← All systems', href='/', style={
            'color': COLORS['muted'],
            'textDecoration': 'none',
            'fontSize': '0.9rem',
        }),
        html.H2(id='system-title', style={
            'margin': '0.5rem 0 0.25rem',
            'fontWeight': '600',
        }),
        html.Div(id='system-meta', style={
            'color': COLORS['muted'],
            'fontSize': '0.9rem',
            'marginBottom': '1.5rem',
        }),

        dcc.Loading(id='alltime-loading', children=[
            html.Div(id='alltime-stats', style={'marginBottom': '1.5rem'}),
        ], type='circle'),

        # Controls
        html.Div([
            html.Div('Custom Range', style={
                'fontWeight': '600',
                'fontSize': '0.85rem',
                'marginBottom': '1rem',
                'color': COLORS['muted'],
                'textTransform': 'uppercase',
                'letterSpacing': '0.05em',
            }),

            # Top row: date, freq, feed
            html.Div([
                html.Div([
                    html.Label('Date Range', style={
                        'fontWeight': '600',
                        'display': 'block',
                        'marginBottom': '0.5rem',
                        'fontSize': '0.85rem',
                    }),
                    dcc.DatePickerRange(
                        id='date-range',
                        display_format='YYYY-MM-DD',
                    ),
                ], style={'marginRight': '2.5rem'}),

                html.Div([
                    html.Label('Frequency', style={
                        'fontWeight': '600',
                        'display': 'block',
                        'marginBottom': '0.5rem',
                        'fontSize': '0.85rem',
                    }),
                    dcc.RadioItems(
                        id='freq-selector',
                        options=[
                            {'label': 'Hourly', 'value': 'h'},
                            {'label': 'Daily', 'value': 'd'},
                            {'label': 'Monthly', 'value': 'm'},
                            {'label': 'Yearly', 'value': 'y'},
                        ],
                        value='h',
                        inline=True,
                        inputStyle={'marginRight': '4px'},
                        labelStyle={'marginRight': '16px', 'fontSize': '0.9rem'},
                    ),
                ], style={'marginRight': '2.5rem'}),

                html.Div([
                    html.Label('Feed', style={
                        'fontWeight': '600',
                        'display': 'block',
                        'marginBottom': '0.5rem',
                        'fontSize': '0.85rem',
                    }),
                    dcc.RadioItems(
                        id='feed-selector',
                        options=[
                            {'label': 'Station', 'value': 'station'},
                            {'label': 'Free Bike', 'value': 'free_bike'},
                        ],
                        value='station',
                        inline=True,
                        inputStyle={'marginRight': '4px'},
                        labelStyle={'marginRight': '16px', 'fontSize': '0.9rem'},
                    ),
                ], id='feed-container'),
            ], style={
                'display': 'flex',
                'alignItems': 'flex-start',
                'flexWrap': 'wrap',
                'gap': '1rem',
            }),

            # Bottom row: station selector
            html.Div([
                html.Label('Station', style={
                    'fontWeight': '600',
                    'display': 'block',
                    'marginBottom': '0.5rem',
                    'fontSize': '0.85rem',
                }),
                dcc.Dropdown(
                    id='station-selector',
                    options=[],
                    value=[],
                    multi=True,
                    placeholder='All Stations',
                    style={'width': '100%'},
                ),
            ], style={'marginTop': '1rem'}),

        ], style={
            **CARD_STYLE,
            'marginBottom': '1.5rem',
            'position': 'sticky',
            'top': '59px',
            'zIndex': '100',
        }),

        dcc.Loading(
            children=[
                html.Div(id='charts-container'),
            ],
            type='circle',
        ),
    ]),

], style={
    'fontFamily': 'system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif',
    'backgroundColor': COLORS['bg'],
    'minHeight': '100vh',
    'color': COLORS['text'],
})


@app.callback(
    Output('frontpage', 'style'),
    Output('systempage', 'style'),
    Output('current-system', 'data'),
    Input('url', 'pathname'),
)
def route(pathname):
    if pathname and pathname.startswith('/system/'):
        system_name = pathname.split('/system/', 1)[1]
        return {'display': 'none'}, {'display': 'block', 'padding': '2rem'}, system_name
    return {'padding': '2rem'}, {'display': 'none'}, None


@app.callback(
    Output('system-grid', 'children'),
    Input('url', 'pathname'),
)
def load_front_page(pathname):
    if pathname and pathname.startswith('/system/'):
        return dash.no_update
    try:
        systems = br.get_systems()
    except Exception as e:
        return html.P(f'Error loading systems: {e}', style={'color': 'red'})

    t2 = dt.datetime.now().replace(hour=23, minute=0, second=0, microsecond=0)
    t1 = t2 - dt.timedelta(days=7)

    systems_sorted = sorted(systems, key=lambda s: (not s.get('tracking', False), s['name']))

    th_style = {
        'padding': '0.6rem 1rem',
        'textAlign': 'left',
        'fontWeight': '600',
        'fontSize': '0.8rem',
        'color': COLORS['muted'],
        'textTransform': 'uppercase',
        'letterSpacing': '0.05em',
        'borderBottom': f'2px solid #e0e0e0',
        'backgroundColor': '#fafafa',
    }
    td_style = {
        'padding': '0.6rem 1rem',
        'borderBottom': '1px solid #f0f0f0',
        'verticalAlign': 'middle',
    }

    rows = []
    for s in systems_sorted:
        tracking = s.get('tracking', False)
        tracking_start = s.get('tracking_start')
        if tracking_start:
            try:
                tracking_start = pd.to_datetime(tracking_start).strftime('%b %d, %Y')
            except Exception:
                pass
        else:
            tracking_start = 'N/A'

        sparkline = _make_sparkline(s['name'], t1, t2) if tracking else html.Div()

        status_badge = html.Span(
            'Active' if tracking else 'Inactive',
            style={
                'padding': '0.2rem 0.6rem',
                'borderRadius': '999px',
                'fontSize': '0.78rem',
                'fontWeight': '600',
                'backgroundColor': '#e8f5e9' if tracking else '#f5f5f5',
                'color': '#2e7d32' if tracking else COLORS['muted'],
            },
        )

        rows.append(
            html.Tr([
                html.Td(
                    dcc.Link(
                        s['name'].replace('_', ' ').title(),
                        href=f'/system/{s["name"]}',
                        style={'color': COLORS['text'], 'textDecoration': 'none', 'fontWeight': '600'},
                    ),
                    style=td_style,
                ),
                html.Td(status_badge, style=td_style),
                html.Td(tracking_start, style={**td_style, 'color': COLORS['muted'], 'fontSize': '0.9rem'}),
                html.Td(sparkline, style={**td_style, 'width': '180px', 'padding': '0.25rem 1rem'}),
            ], style={'backgroundColor': 'white'}, className='system-row')
        )

    return html.Div(
        html.Table([
            html.Thead(html.Tr([
                html.Th('System', style=th_style),
                html.Th('Status', style=th_style),
                html.Th('Tracking Since', style=th_style),
                html.Th('Last 7 Days', style=th_style),
            ])),
            html.Tbody(rows),
        ], style={
            'width': '100%',
            'borderCollapse': 'collapse',
            'borderRadius': '8px',
            'overflow': 'hidden',
            'boxShadow': '0 1px 4px rgba(0,0,0,0.08)',
        }),
        style={'overflowX': 'auto'},
    )


@app.callback(
    Output('system-title', 'children'),
    Output('system-meta', 'children'),
    Input('current-system', 'data'),
)
def update_system_header(system_name):
    if not system_name:
        return '', ''
    try:
        api = br.LiveAPI(system_name)
        info = api.info
        tracking_start = info.get('tracking_start')
        if tracking_start:
            try:
                tracking_start = pd.to_datetime(tracking_start).strftime('%B %d, %Y')
            except Exception:
                pass
        tz = info.get('tz', 'N/A')
        return (
            system_name.replace('_', ' ').title(),
            f'Timezone: {tz}  ·  Tracking since: {tracking_start or "N/A"}',
        )
    except Exception as e:
        return system_name, f'Error: {e}'


@app.callback(
    Output('feed-container', 'style'),
    Output('feed-selector', 'value'),
    Input('current-system', 'data'),
)
def update_feed_visibility(system_name):
    if not system_name:
        return {}, 'station'
    try:
        api = br.LiveAPI(system_name)
        has_free_bike = api.info.get('track_free_bikes', True)
    except Exception:
        has_free_bike = True
    style = {} if has_free_bike else {'display': 'none'}
    return style, 'station'


@app.callback(
    Output('station-selector', 'options'),
    Output('station-selector', 'value'),
    Input('current-system', 'data'),
    dash.State('url', 'search'),
)
def populate_station_dropdown(system_name, search):
    if not system_name:
        return [], []
    try:
        api = br.LiveAPI(system_name)
        station_info = api.get_stations()
    except Exception:
        return [], []
    if station_info is None or len(station_info) == 0:
        return [], []
    options = [
        {'label': row['name'], 'value': row['station_id']}
        for _, row in station_info.sort_values('name').iterrows()
    ]
    params = parse_qs((search or '').lstrip('?'))
    selected = params.get('station', [])
    valid_ids = {o['value'] for o in options}
    selected = [s for s in selected if s in valid_ids]
    return options, selected


@app.callback(
    Output('date-range', 'start_date'),
    Output('date-range', 'end_date'),
    Input('current-system', 'data'),
    dash.State('url', 'search'),
)
def set_default_dates(system_name, search):
    if not system_name:
        return dash.no_update, dash.no_update
    params = parse_qs((search or '').lstrip('?'))
    try:
        start = dt.date.fromisoformat(params['start'][0])
    except (KeyError, ValueError):
        start = dt.date.today() - dt.timedelta(days=7)
    try:
        end = dt.date.fromisoformat(params['end'][0])
    except (KeyError, ValueError):
        end = dt.date.today()
    return start.isoformat(), end.isoformat()


@app.callback(
    Output('alltime-stats', 'children'),
    Input('current-system', 'data'),
)
def update_alltime_stats(system_name):
    if not system_name:
        return html.Div()
    try:
        api = br.LiveAPI(system_name)
        info = api.info
        tracking_start = pd.to_datetime(info.get('tracking_start'), utc=True)
        tracking_end = pd.to_datetime(info.get('tracking_end'), utc=True)

        t1 = tracking_start.to_pydatetime().replace(tzinfo=None)
        t2 = tracking_end.to_pydatetime().replace(tzinfo=None)
        df = api.get_trips(t1, t2, freq='y', feed='station')
    except Exception as e:
        return html.P(f'Error loading all-time stats: {e}', style={'color': 'red'})

    if df is None or len(df) == 0:
        return html.Div()

    df = df.reset_index()
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)
    df['year'] = df['datetime'].dt.year
    total_trips = int(df['trips'].sum())
    total_returns = int(df['returns'].sum())

    days_tracked = (tracking_end - tracking_start).days
    avg_daily = round(total_trips / days_tracked) if days_tracked > 0 else 0

    tracking_start_fmt = tracking_start.strftime('%b %d, %Y')
    tracking_end_fmt = tracking_end.strftime('%b %d, %Y')

    yearly_fig = go.Figure([
        go.Bar(x=df['year'], y=df['trips'], name='Trips', marker_color=COLORS['trips']),
    ])
    yearly_fig.update_layout(
        template='plotly_white',
        paper_bgcolor='white',
        margin={'t': 10, 'b': 40, 'l': 50, 'r': 10},
        height=200,
        showlegend=False,
        xaxis={'tickmode': 'linear', 'dtick': 1},
        yaxis={'title': 'Trips'},
    )

    return html.Div([
        html.Div('All-time Summary', style={
            'fontWeight': '600',
            'fontSize': '0.85rem',
            'marginBottom': '1rem',
            'color': COLORS['muted'],
            'textTransform': 'uppercase',
            'letterSpacing': '0.05em',
        }),
        html.Div([
            html.Div([
                html.Div([
                    _stat_card('Total Trips', f'{total_trips:,}', COLORS['trips']),
                    _stat_card('Total Returns', f'{total_returns:,}', COLORS['returns']),
                    _stat_card('Avg Daily Trips', f'{avg_daily:,}', '#4CAF50'),
                    _stat_card('Days Tracked', f'{days_tracked:,}', '#9C27B0'),
                    _stat_card('Period', f'{tracking_start_fmt} – {tracking_end_fmt}', COLORS['muted']),
                ], style={'display': 'flex', 'gap': '1rem', 'flexWrap': 'wrap', 'marginBottom': '1rem'}),
                dcc.Graph(figure=yearly_fig, config={'displayModeBar': False}),
            ]),
        ]),
    ], style={**CARD_STYLE, 'marginBottom': '1.5rem'})


@app.callback(
    Output('charts-container', 'children'),
    Input('current-system', 'data'),
    Input('date-range', 'start_date'),
    Input('date-range', 'end_date'),
    Input('freq-selector', 'value'),
    Input('feed-selector', 'value'),
    Input('station-selector', 'value'),
)
def update_charts(system_name, start_date, end_date, freq, feed, station):
    if not system_name or not start_date or not end_date:
        return html.Div()

    no_data_div = html.Div('Data not available for selected period.', style={
        'padding': '1rem 1.5rem',
        'backgroundColor': '#fff8e1',
        'border': '1px solid #ffe082',
        'borderRadius': '8px',
        'color': '#f57f17',
        'fontSize': '0.95rem',
    })

    station = station or []
    try:
        api = br.LiveAPI(system_name)
        t1 = dt.datetime.fromisoformat(start_date)
        t2 = dt.datetime.fromisoformat(end_date).replace(hour=23)
        df_stations = api.get_trips(t1, t2, freq=freq, feed=feed, station='all')
        if station:
            if df_stations is not None and len(df_stations) > 0:
                df_st_filtered = df_stations.reset_index()
                df_st_filtered = df_st_filtered[df_st_filtered['station_id'].isin(station)]
                df_agg = df_st_filtered.groupby('datetime')[['trips', 'returns']].sum()
            else:
                df_agg = None
        else:
            df_agg = api.get_trips(t1, t2, freq=freq, feed=feed)
        station_info = api.get_stations()
    except ValueError:
        return no_data_div
    except Exception as e:
        return html.P(f'Error fetching data: {e}', style={'color': 'red'})

    if df_agg is None or len(df_agg) == 0:
        return no_data_div

    df_agg = df_agg.reset_index()
    df_agg['datetime'] = pd.to_datetime(df_agg['datetime'], utc=True)
    total_trips = int(df_agg['trips'].sum())
    total_returns = int(df_agg['returns'].sum())
    peak_trips = int(df_agg['trips'].max())

    summary = html.Div([
        _stat_card('Total Trips', f'{total_trips:,}', COLORS['trips']),
        _stat_card('Total Returns', f'{total_returns:,}', COLORS['returns']),
        _stat_card('Peak in Period', f'{peak_trips:,}', '#4CAF50'),
    ], style={'display': 'flex', 'gap': '1rem', 'flexWrap': 'wrap'})

    # Timeline chart
    freq_label = {'h': 'Hourly', 'd': 'Daily', 'm': 'Monthly', 'y': 'Yearly'}.get(freq, freq)
    mode = 'lines+markers' if len(df_agg) <= 60 else 'lines'
    timeline_fig = go.Figure([
        go.Scatter(
            x=df_agg['datetime'], y=df_agg['trips'],
            mode=mode, name='Trips',
            line={'color': COLORS['trips'], 'width': 2},
            fill='tozeroy', fillcolor='rgba(33,150,243,0.08)',
        ),
        go.Scatter(
            x=df_agg['datetime'], y=df_agg['returns'],
            mode=mode, name='Returns',
            line={'color': COLORS['returns'], 'width': 2},
            fill='tozeroy', fillcolor='rgba(255,152,0,0.08)',
        ),
    ])
    timeline_fig.update_layout(
        title=f'{freq_label} Trips & Returns',
        xaxis_title='Date/Time',
        yaxis_title='Count',
        template='plotly_white',
        paper_bgcolor='white',
        legend={'orientation': 'h', 'y': -0.15},
        margin={'t': 50, 'b': 60},
        height=320,
    )

    # Station breakdown chart
    if df_stations is not None and len(df_stations) > 0:
        df_st = df_stations.reset_index()
        df_st['datetime'] = pd.to_datetime(df_st['datetime'], utc=True)

        if station:
            df_st = df_st[df_st['station_id'].isin(station)]

        if 'station_id' in df_st.columns and df_st['station_id'].notna().any():
            st_agg = (
                df_st.groupby('station_id')[['trips', 'returns']]
                .sum()
                .pipe(lambda d: d[d['trips'] > 0])
                .sort_values('trips', ascending=True)
                .tail(25)
            )
            if station_info is not None:
                name_map = station_info.set_index('station_id')['name'].to_dict()
                labels = [name_map.get(sid, sid) for sid in st_agg.index]
            else:
                labels = st_agg.index.astype(str).tolist()

            bar_fig = go.Figure([
                go.Bar(
                    y=labels, x=st_agg['trips'],
                    name='Trips', orientation='h', marker_color=COLORS['trips'],
                ),
                go.Bar(
                    y=labels, x=st_agg['returns'],
                    name='Returns', orientation='h', marker_color=COLORS['returns'],
                ),
            ])
            bar_fig.update_layout(
                title='Stations by Trips' if station else 'Top 25 Stations by Trips',
                xaxis_title='Count',
                yaxis_title='Station',
                barmode='group',
                template='plotly_white',
                paper_bgcolor='white',
                height=max(350, len(st_agg) * 28),
                margin={'t': 50, 'l': 100},
            )
        else:
            bar_fig = _empty_fig('No station breakdown available')
    else:
        bar_fig = _empty_fig('No station breakdown available')

    # Map
    map_div = html.Div()
    if df_stations is not None and station_info is not None and \
            {'lat', 'lon'}.issubset(station_info.columns):
        df_st_map = df_stations.reset_index()
        if station:
            df_st_map = df_st_map[df_st_map['station_id'].isin(station)]
        st_map = (
            df_st_map.groupby('station_id')[['trips', 'returns']]
            .sum()
            .reset_index()
            .merge(station_info[['station_id', 'name', 'lat', 'lon']], on='station_id', how='left')
        )
        st_map = st_map[st_map['lat'].notna() & st_map['trips'] > 0]
        if len(st_map) > 0:
            max_trips = st_map['trips'].max()
            sizes = ((st_map['trips'] / max_trips) * 32 + 8).tolist()
            map_fig = go.Figure(go.Scattermapbox(
                lat=st_map['lat'],
                lon=st_map['lon'],
                mode='markers',
                marker=go.scattermapbox.Marker(
                    size=sizes,
                    color=COLORS['trips'],
                    opacity=0.7,
                ),
                text=st_map['name'],
                customdata=st_map['trips'],
                hovertemplate='<b>%{text}</b><br>Trips: %{customdata:,}<extra></extra>',
            ))
            map_fig.update_layout(
                mapbox_style='open-street-map',
                mapbox=dict(
                    center=dict(lat=st_map['lat'].mean(), lon=st_map['lon'].mean()),
                    zoom=11,
                ),
                margin={'t': 0, 'b': 0, 'l': 0, 'r': 0},
                height=450,
                paper_bgcolor='white',
            )
            map_div = dcc.Graph(
                figure=map_fig,
                config={'doubleClick': False},
                style={'marginBottom': '1.5rem'},
            )

    return html.Div([
        html.Div(summary, style={'marginBottom': '1.5rem'}),
        dcc.Graph(figure=timeline_fig, style={'marginBottom': '1.5rem'}),
        map_div,
        dcc.Graph(figure=bar_fig),
    ])


@app.callback(
    Output('url', 'search'),
    Input('date-range', 'start_date'),
    Input('date-range', 'end_date'),
    Input('station-selector', 'value'),
    dash.State('current-system', 'data'),
    prevent_initial_call=True,
)
def update_url_search(start_date, end_date, station, system_name):
    if not system_name or not start_date or not end_date:
        return dash.no_update
    qs = f'?start={start_date}&end={end_date}'
    if station:
        qs += '&' + '&'.join(f'station={s}' for s in station)
    return qs


def _make_sparkline(system_name, t1, t2):
    try:
        api = br.LiveAPI(system_name)
        df = api.get_trips(t1, t2, freq='h', feed='station')
    except Exception:
        df = None

    if df is None or len(df) == 0:
        return html.Div(style={'height': '50px'})

    df = df.reset_index()
    df['datetime'] = pd.to_datetime(df['datetime'], utc=True)

    fig = go.Figure(go.Scatter(
        x=df['datetime'], y=df['trips'],
        mode='lines',
        line={'color': COLORS['trips'], 'width': 1.5},
        fill='tozeroy',
        fillcolor='rgba(33,150,243,0.12)',
    ))
    fig.update_layout(
        margin={'t': 0, 'b': 0, 'l': 0, 'r': 0},
        height=50,
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        xaxis={'visible': False},
        yaxis={'visible': False},
        showlegend=False,
    )
    return dcc.Graph(
        figure=fig,
        config={'displayModeBar': False, 'staticPlot': True},
        style={'height': '50px', 'pointerEvents': 'none'},
    )


def _stat_card(label, value, color):
    return html.Div([
        html.Div(value, style={
            'fontSize': '1.75rem',
            'fontWeight': '700',
            'color': color,
        }),
        html.Div(label, style={
            'fontSize': '0.82rem',
            'color': COLORS['muted'],
            'marginTop': '0.2rem',
        }),
    ], style={
        **CARD_STYLE,
        'padding': '1rem 1.5rem',
        'minWidth': '140px',
        'borderTop': f'3px solid {color}',
    })


def _empty_fig(msg=None):
    fig = go.Figure()
    fig.update_layout(
        template='plotly_white',
        paper_bgcolor='white',
        annotations=[{
            'text': msg or '',
            'showarrow': False,
            'font': {'size': 14, 'color': '#aaa'},
            'xref': 'paper', 'yref': 'paper',
            'x': 0.5, 'y': 0.5,
        }] if msg else [],
    )
    return fig


if __name__ == '__main__':
    app.run(debug=True)
