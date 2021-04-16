import datetime
import streamlit as st
import pandas as pd
import altair as alt
from vega_datasets import data

@st.cache
def load_data():
    data = pd.read_csv("../../data/teste.csv", delimiter=";",index_col='job_name')
    data = data[['user_name','started','time_allocation','time_run']]
    data['started'] = pd.to_datetime(data['started'])
    return data

# carregar os dados

df_data = load_data()


# SIDEBAR
# Parâmetros e número de ocorrências
st.sidebar.header("Parâmetros")
info_sidebar = st.sidebar.empty() 

# Slider de seleção do ano

possible_dates = df_data['started'].dt.date.unique()
today = datetime.date(2021, 3, 13)
tomorrow = datetime.date(2021, 3, 11)
start_date = st.sidebar.date_input('Start date', value=tomorrow, min_value=possible_dates[0],  max_value=possible_dates[-1])
end_date = st.sidebar.date_input('End date', value=today, min_value=possible_dates[0],  max_value=possible_dates[-1])
if start_date > end_date:
    st.sidebar.error('Error: End date must fall after start date.')

if start_date not in possible_dates or end_date not in possible_dates:
    st.sidebar.rwarning( f'Date data not available. Try one of these: {possible_dates}' + 'Changed date to ' + possible_dates[0] + '.' )
    date = possible_dates[0]

# Multiselect com os lables únicos dos tipos de classificação
labels = df_data['user_name'].unique().tolist()
user_to_filter = st.sidebar.multiselect(
    label="Escolha o usuário",
    options=labels,
    default=["ioper"]
)


# Checkbox da Tabela
st.sidebar.subheader("Tabela")
tabela = st.sidebar.empty()


# Somente aqui os dados filtrados por ano são atualizados em novo dataframe
filtered_df = df_data[(df_data['started'].dt.date >= start_date) &  (df_data['started'].dt.date <= end_date) & (df_data['user_name'].isin(user_to_filter))]

# Aqui o placehoder vazio finalmente é atualizado com dados do filtered_df
info_sidebar.info("{} ocorrências selecionadas.".format(filtered_df.shape[0]))

# MAIN
st.title("Jobs - PBS")
st.markdown(f""" Estão sendo exibidas logs de **{", ".join(user_to_filter)}** de **{start_date}** até **{end_date}**  """)



filtered_data = df_data[(df_data['started'].dt.date >= start_date) &  (df_data['started'].dt.date <= end_date) & (df_data['user_name'].isin(user_to_filter))]

st.subheader('Time Run (minutes)')
chart_time_run = alt.Chart(filtered_data).mark_bar().encode(
    x='monthdate(started):O',
    y='sum(time_run)',
    color='user_name'
)
st.altair_chart(chart_time_run, use_container_width=True) 

st.subheader('Time Allocation (minutes)')
chart_time_queue = alt.Chart(filtered_data).mark_bar().encode(
    x='monthdate(started):O',
    y='sum(time_allocation)',
    color='user_name'
)
st.altair_chart(chart_time_queue, use_container_width=True) 


# raw data (tabela) dependente do checkbox
if tabela.checkbox("Mostrar tabela de dados"):
    st.write(filtered_df)