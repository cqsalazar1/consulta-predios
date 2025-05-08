import os
import folium
import sqlite3
import pandas as pd
import psycopg2
import streamlit as st
import geopandas as gpd
import leafmap.foliumap as leafmap
from shapely import wkt
from shapely.geometry import Point

st.set_page_config(page_title='Consulta de Predios', layout='centered', page_icon="🔍")
st.subheader("Consulta Predial", divider='gray')

m = leafmap.Map(
    center=[3.4248559, -76.5188715],
    zoom=12,
    zoom_control=True,
    draw_control=False,
    scale_control=False,
    layers_control=True,
    fullscreen_control=False,
    measure_control=False,
    toolbar_control=False
)

# Cargue de información cartográfica
@st.cache_data
def load_data(option, input):
   conexion1 = sqlite3.connect('Terrenos_Part1.db')
   conexion2 = sqlite3.connect('Terrenos_Part2.db')
   query1 = f"SELECT * FROM Terrenos_28032025_Part1_spatial WHERE {option} = '{input}'"
   query2 = f"SELECT * FROM Terrenos_28032025_Part2_spatial WHERE {option} = '{input}'"
   df1 = pd.read_sql_query(query1, conexion1)
   df1['geometry'] = df1['geometry'].apply(wkt.loads)
   df2 = pd.read_sql_query(query2, conexion2)
   df2['geometry'] = df2['geometry'].apply(wkt.loads)
   if df1.shape[0] > 0:
       gdf = gpd.GeoDataFrame(df1, geometry='geometry', crs='4326')
   elif df2.shape[0] > 0:
       gdf = gpd.GeoDataFrame(df2, geometry='geometry', crs='4326')
   conexion1.close()
   conexion2.close()
   return gdf

@st.cache_data
def load_data_intersect(latitud, longitud):
    try:
        conexion = psycopg2.connect(
            host="terrenos-cqsalazar.l.aivencloud.com",  # Cambia por tu host
            database="defaultdb",  # Cambia por el nombre de tu base de datos
            user="avnadmin",  # Cambia por tu usuario
            password="AVNS_e0Ycamzw95h4-x8RzO5", # Cambia por tu contraseña
            port="25365"  
        )
        cursor = conexion.cursor() # Crear un cursor para ejecutar consultas

        consulta = f""" SELECT *, ST_AsText(geometry) AS wkt FROM terrenos WHERE ST_Intersects(geometry, 'SRID=4326; POINT({longitud} {latitud})'::geometry) """
        
        cursor.execute(consulta)

        columnas = [col[0] for col in cursor.description]  # Obtener nombres de columnas
        df = pd.DataFrame(cursor.fetchall(), columns=columnas)
        df['wkt'] = df['wkt'].apply(wkt.loads)
        gdf = gpd.GeoDataFrame(df, geometry='wkt', crs='4326')
        return gdf
        
    except psycopg2.Error as e:
        print(f"Error al conectar o consultar la base de datos: {e}")

    finally:
        if cursor:
            cursor.close()
        if conexion:
            conexion.close()

# Cargue de información alfanumérica
@st.cache_data
def load_table(option, input):
    conexion = sqlite3.connect('export_alfanumerica.db')
    cursor = conexion.cursor()
    cursor.execute(f"SELECT * FROM export_predio_09032025 WHERE {option} = '{input}'")
    data = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    df = pd.DataFrame(data, columns=columns)
    gdf = gpd.GeoDataFrame(df)
    conexion.close()
    return gdf

## CONSULTAS
option = st.sidebar.selectbox(
    "Seleccione el tipo de consulta",
    ("ID PREDIO", "NÚMERO PREDIAL", "NPN", "COORDENADAS"),
)

if option == 'ID PREDIO':
    option1 = 'IDPREDIO'
    option2 = 'ID_PREDIO'    
    try:
        filtro_id_predio = st.sidebar.number_input("ID PREDIO:", value=None, min_value=0, placeholder=0)
        if filtro_id_predio:
            selected_gdf = load_data(option1, filtro_id_predio)
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
            st.markdown(":gray[**Información Alfanumérica**]")
            df_filtrado = load_table(option2, filtro_id_predio)
            if len(df_filtrado) == 0:
                st.markdown(":gray[*El ID PREDIO no se encontró en la base alfanumérica*]")
            else:
                st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        df_filtrado = load_table(option2, filtro_id_predio)
        st.markdown(":gray[**Información Alfanumérica**]")
        if len(df_filtrado) == 0:
                st.markdown(":gray[*El ID PREDIO no se encontró en la base alfanumérica*]")
        else:
                st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        st.sidebar.markdown(":gray[*El ID PREDIO no se encontró en la base cartográfica*]")

elif option == 'NÚMERO PREDIAL':
    option1 = 'NUMEPRED'
    option2 = 'NUMERO_PREDIAL'
    try:
        filtro_num_pred = st.sidebar.text_input("NÚMERO PREDIAL:")
        if filtro_num_pred:
            selected_gdf = load_data(option1, filtro_num_pred)
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/map:", use_container_width=True)
            st.markdown(":gray[**Información Alfanumérica**]")
            df_filtrado = load_table(option2, filtro_num_pred)
            if len(df_filtrado) == 0:
                st.markdown(":gray[*El Número Predial no se encontró en la base alfanumérica*]")
            else:
                st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        st.markdown(":gray[**Información Alfanumérica**]")
        df_filtrado = load_table(option2, filtro_num_pred)
        if len(df_filtrado) == 0:
                st.markdown(":gray[*El Número Predial no se encontró en la base alfanumérica*]")
        else:
            st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        st.sidebar.markdown(":gray[*El Número Predial no se encontró en la base cartográfica*]")

elif option == 'NPN':
    try:
        filtro_npn = st.sidebar.text_input("NPN:")
        if filtro_npn:
            selected_gdf = load_data(option, filtro_npn)
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/map:", use_container_width=True)
            st.markdown(":gray[**Información Alfanumérica**]")
            df_filtrado = load_table(option, filtro_npn)
            if len(df_filtrado) == 0:
                st.markdown(":gray[*El Número Predial Nacional (NPN) no se encontró en la base alfanumérica*]")
            else:
                st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        st.markdown(":gray[**Información Alfanumérica**]")
        df_filtrado = load_table(option, filtro_npn)
        if len(df_filtrado) == 0:
                st.markdown(":gray[*El Número Predial Nacional (NPN) no se encontró en la base alfanumérica*]")
        else:
            st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        st.sidebar.markdown(":gray[*El Número Predial Nacional (NPN) no se encontró en la base cartográfica*]")

elif option == 'COORDENADAS':
    option = 'ID_PREDIO'
    try:
        coordenadas = st.sidebar.text_input("COORDENADAS:", placeholder="3.4248559, -76.5188715")
        if coordenadas:
            coordenadas = coordenadas.split(',')
            latitud = float(coordenadas[0])
            longitud = float(coordenadas[1].strip())
            # folium.Marker(
            #         location=[latitud, longitud],
            #         popup=f"{latitud}, {longitud}",
            #         icon=folium.Icon(color="green", icon="home")
            # ).add_to(m)

            coordenadas = [(latitud, longitud)]
            df_point = pd.DataFrame([(latitud, longitud)], columns=['Latitud', 'Longitud'])
            df_point['geometry'] = df_point.apply(lambda row: Point(row['Longitud'], row['Latitud']), axis=1) # Convertir las coordenadas en objetos Point 
            gdf_point = gpd.GeoDataFrame(df_point, geometry='geometry') # Crear el GeoDataFrame
            gdf_point.set_crs(epsg=4326, inplace=True) # Establecer el sistema de referencia de coordenadas (CRS)           

            selected_gdf = load_data_intersect(latitud, longitud)
        
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.markdown(":gray[**Información Alfanumérica**]")
            df_filtrado = load_table(option, int(selected_gdf['IDPREDIO']))
            st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/map:", use_container_width=True)
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        st.sidebar.markdown(":gray[*No se encontró ningún predio en las coordenadas aportadas*]")
