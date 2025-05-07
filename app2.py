import os
import folium
import sqlite3
import pandas as pd
import streamlit as st
import geopandas as gpd
import leafmap.foliumap as leafmap
from shapely import wkt
from shapely.geometry import Point

st.set_page_config(page_title='Consulta de Predios', layout='centered', page_icon="🔍")
st.subheader("Consulta Predial", divider='gray')

crs = 'EPSG:4326'
data_folder = 'data'
gpkg_file = 'consulta_predios.gpkg'
gpkg_filepath = os.path.join(data_folder, gpkg_file)

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
gpkg_file = 'consulta_predios.gpkg'
gpkg_filepath = os.path.join(data_folder, gpkg_file)

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


#    conexion = sqlite3.connect('consulta_predios.db')
#    query = f"SELECT * FROM Terrenos_28032025_spatial WHERE {option} = '{input}'"
#    df = pd.read_sql_query(query, conexion)
#    df['geometry'] = df['geometry'].apply(wkt.loads)
#    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='4326')
#    conexion.close()
#    return gdf

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
    option1 = 'ID_PREDIO'
    option2 = 'IDPREDIO'
    try:
        filtro_id_predio = st.sidebar.number_input("ID PREDIO:", value=None, min_value=0, placeholder=0)
        if filtro_id_predio:
            selected_gdf = load_data(option2, filtro_id_predio)
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
            st.markdown(":gray[**Información Alfanumérica**]")
            df_filtrado = load_table(option1, filtro_id_predio)
            if len(df_filtrado) == 0:
                st.markdown(":gray[*El ID PREDIO no se encontró en la base alfanumérica*]")
            else:
                st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        df_filtrado = load_table(option1, filtro_id_predio)
        st.markdown(":gray[**Información Alfanumérica**]")
        if len(df_filtrado) == 0:
                st.markdown(":gray[*El ID PREDIO no se encontró en la base alfanumérica*]")
        else:
                st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        st.sidebar.markdown(":gray[*El ID PREDIO no se encontró en la base cartográfica*]")

elif option == 'NÚMERO PREDIAL':
    option = 'NUMERO_PREDIAL'
    try:
        filtro_num_pred = st.sidebar.text_input("NÚMERO PREDIAL:")
        if filtro_num_pred:
            selected_gdf = gdf[(gdf['NUMEPRED'] == filtro_num_pred)]
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/map:", use_container_width=True)
            st.markdown(":gray[**Información Alfanumérica**]")
            df_filtrado = load_table(option, filtro_num_pred)
            if len(df_filtrado) == 0:
                st.markdown(":gray[*El Número Predial Nacional (NPN) no se encontró en la base alfanumérica*]")
            else:
                st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        st.markdown(":gray[**Información Alfanumérica**]")
        df_filtrado = load_table(option, filtro_num_pred)
        if len(df_filtrado) == 0:
                st.markdown(":gray[*El Número Predial Nacional (NPN) no se encontró en la base alfanumérica*]")
        else:
            st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        st.sidebar.markdown(":gray[*El Número Predial no se encontró en la base cartográfica*]")

elif option == 'NPN':
    try:
        filtro_npn = st.sidebar.text_input("NPN:")
        if filtro_npn:
            selected_gdf = gdf[(gdf['NPN'] == filtro_npn)]
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

            gdf_temp = gpd.overlay(gdf_point, gdf, how='intersection', keep_geom_type=None, make_valid=True) # Geodataframe temporal resultante de la interseccción con el punto
            selected_gdf = gdf[(gdf['IDPREDIO'] == int(gdf_temp['IDPREDIO']))] # Seleccionar predio a partir del Geodataframe temporal
        
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.markdown(":gray[**Información Alfanumérica**]")
            df_filtrado = load_table(option, int(gdf_temp['IDPREDIO']))
            st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/map:", use_container_width=True)
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        st.sidebar.markdown(":gray[*No se encontró ningún predio en las coordenadas aportadas*]")


#3.4571888, -76.4970199
# comunas = gpd.read_file(gpkg_filepath, layer='Comunas')
# m.add_gdf(comunas, layer_name='Comunas', style={'color':'gray', 'fill':'white', 'weight':1})
