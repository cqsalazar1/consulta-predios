import psycopg2
import pandas as pd
import streamlit as st
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point
import leafmap.foliumap as leafmap

# Conexion con base de datos de Postgres alojada en Aiven
def conectar_bd():
    try:
        conexion = psycopg2.connect(
            host=st.secrets['AIVEN_HOST'],
            database=st.secrets['AIVEN_DATABASE'],
            user=st.secrets['AIVEN_USER'],
            password=st.secrets['AIVEN_PASSWORD'],
            port=st.secrets['AIVEN_PORT'] 
        )
        return conexion    

    except psycopg2.Error as e:
        print(f"Error al conectar o consultar la base de datos: {e}")

# Cargue de información alfanumérica
@st.cache_data
def load_table(_conexion, option, input):
    cursor = _conexion.cursor() # Crear un cursor para ejecutar consultas
    consulta = f""" SELECT * FROM "export_MAESTRO_predio_09032025" WHERE "{option}" = '{input}' """

    cursor.execute(consulta)

    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(data, columns=columns)
    gdf = gpd.GeoDataFrame(df)
    gdf = gdf.drop(columns=['index'])
    return gdf
    
# Cargue de información cartográfica
@st.cache_data
def load_predio(_conexion, option, input):
    cursor = _conexion.cursor() # Crear un cursor para ejecutar consultas
    consulta = f""" SELECT *, ST_AsText(geometry) AS wkt FROM terrenos WHERE "{option}" = '{input}' """

    cursor.execute(consulta)

    columnas = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(cursor.fetchall(), columns=columnas)
    df['wkt'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='wkt', crs='4326')
    gdf = gdf.drop(columns=['OBJECTID', 'ETIQUETA', 'IDTERRENO', 'POLITERID', 'OBS', 'LAST_EDITE', 'TIPO_AVALU', 'MODIFICACI', 'EDITOR', 'Shape_Leng', 'geometry'])
    return gdf

@st.cache_data
def load_predio_intersect(_conexion, latitud, longitud):
    cursor = _conexion.cursor() # Crear un cursor para ejecutar consultas
    consulta = f""" SELECT *, ST_AsText(geometry) AS wkt FROM terrenos WHERE ST_Intersects(geometry, 'SRID=4326; POINT({longitud} {latitud})'::geometry) """
        
    cursor.execute(consulta)

    columnas = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(cursor.fetchall(), columns=columnas)
    df['wkt'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='wkt', crs='4326')
    gdf = gdf.drop(columns=['OBJECTID', 'ETIQUETA', 'IDTERRENO', 'POLITERID', 'OBS', 'LAST_EDITE', 'TIPO_AVALU', 'MODIFICACI', 'EDITOR', 'Shape_Leng', 'geometry'])
    return gdf

@st.cache_data
def load_manzana(_conexion, id_manzana):
    cursor = _conexion.cursor() # Crear un cursor para ejecutar consultas
    consulta = f""" SELECT *, ST_AsText(geometry) AS wkt FROM terrenos WHERE "CONEXION" LIKE '{id_manzana}%' """

    cursor.execute(consulta)

    columnas = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(cursor.fetchall(), columns=columnas)
    df['wkt'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='wkt', crs='4326')
    gdf = gdf.drop(columns=['OBJECTID', 'DEPAPRED', 'MUNIPRED', 'ZONA', 'SECTOR', 'COMUNA', 'BARRIO', 'MANZANA', 'TERRENO', 'CONDICION', 'EDIFPRED', 'PISOPRED', 'PREDIO',
                            'POLITERID', 'ETIQUETA',  'IDTERRENO', 'OBS', 'LAST_EDITE', 'TIPO_AVALU', 'MODIFICACI', 'EDITOR', 'Shape_Leng', 'LATITUD', 'LONGITUD', 'geometry'])
    return gdf

@st.cache_data
def load_vecino(_conexion, option, input):
    cursor = _conexion.cursor() # Crear un cursor para ejecutar consultas
    consulta = f""" SELECT *, ST_AsText(geometry) AS wkt FROM terrenos WHERE ST_Touches(geometry, (SELECT geometry FROM terrenos WHERE "{option}" = '{input}')) """
    
    cursor.execute(consulta)
    
    columnas = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(cursor.fetchall(), columns=columnas)
    df['wkt'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='wkt', crs='4326')
    gdf = gdf.drop(columns=['OBJECTID', 'DEPAPRED', 'MUNIPRED', 'ZONA', 'SECTOR', 'COMUNA', 'BARRIO', 'MANZANA', 'TERRENO', 'CONDICION', 'EDIFPRED', 'PISOPRED', 'PREDIO',
                            'POLITERID', 'ETIQUETA',  'IDTERRENO', 'OBS', 'LAST_EDITE', 'TIPO_AVALU', 'MODIFICACI', 'EDITOR', 'Shape_Leng', 'LATITUD', 'LONGITUD', 'geometry'])
    return gdf

st.set_page_config(page_title='Consulta de Predios', layout='centered', page_icon="🔍")

if st.sidebar.button('Salir', type='secondary', use_container_width=True):
    link = "[share](/?param=value)"
    st.markdown("""
        <meta http-equiv="refresh" content="0; url='https://www.google.com'" />
        """, unsafe_allow_html=True
    )

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

conexion = conectar_bd()

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
            selected_gdf = load_predio(conexion, option1, filtro_id_predio)
            if int(selected_gdf['COMUNA'][0]) <= 22:
                vecinos = load_manzana(conexion, selected_gdf['CONEXION'][0][:-4])
            else:
                vecinos = load_vecino(conexion, option1, filtro_id_predio)
            m.add_gdf(vecinos, layer_name='Predios', zoom_to_layer=False, style={'color':'gray', 'fill':'gray', 'weight':1})
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
            st.markdown(":gray[**Información Alfanumérica**]")
            select_df = load_table(conexion, option2, filtro_id_predio)
            if len(select_df) == 0:
                st.markdown(":gray[*El ID PREDIO no se encontró en la base alfanumérica*]")
            else:
                st.data_editor(select_df, key="my_key", num_rows="fixed")
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        st.markdown(":gray[**Información Alfanumérica**]")
        select_df = load_table(conexion, option2, filtro_id_predio)
        if len(select_df) == 0:
                st.markdown(":gray[*El ID PREDIO no se encontró en la base alfanumérica*]")
        else:
               st.data_editor(select_df, key="my_key", num_rows="fixed")
        st.sidebar.markdown(":gray[*El ID PREDIO no se encontró en la base cartográfica*]")

elif option == 'NÚMERO PREDIAL':
    option1 = 'NUMEPRED'
    option2 = 'NUMERO_PREDIAL'
    try:
        filtro_num_pred = st.sidebar.text_input("NÚMERO PREDIAL:")
        if filtro_num_pred:
            selected_gdf = load_predio(conexion, option1, filtro_num_pred)
            if int(selected_gdf['COMUNA'][0]) <= 22:
                vecinos = load_manzana(conexion, selected_gdf['CONEXION'][0][:-4])
            else:
                vecinos = load_vecino(conexion, option1, filtro_num_pred)
            m.add_gdf(vecinos, layer_name='Predios', zoom_to_layer=False, style={'color':'gray', 'fill':'gray', 'weight':1})
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
            st.markdown(":gray[**Información Alfanumérica**]")
            df_filtrado = load_table(conexion, option2, filtro_num_pred)
            if len(df_filtrado) == 0:
                st.markdown(":gray[*El Número Predial no se encontró en la base alfanumérica*]")
            else:
                st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        st.markdown(":gray[**Información Alfanumérica**]")
        df_filtrado = load_table(conexion, option2, filtro_num_pred)
        if len(df_filtrado) == 0:
                st.markdown(":gray[*El Número Predial no se encontró en la base alfanumérica*]")
        else:
            st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        st.sidebar.markdown(":gray[*El Número Predial no se encontró en la base cartográfica*]")

elif option == 'NPN':
    try:
        filtro_npn = st.sidebar.text_input("NPN:")
        if filtro_npn:
            selected_gdf = load_predio(conexion, option, filtro_npn)
            if int(selected_gdf['COMUNA'][0]) <= 22:
                vecinos = load_manzana(conexion, selected_gdf['CONEXION'][0][:-4])
            else:
                vecinos = load_vecino(conexion, option, filtro_npn)
            m.add_gdf(vecinos, layer_name='Predios', zoom_to_layer=False, style={'color':'gray', 'fill':'gray', 'weight':1})
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
            st.markdown(":gray[**Información Alfanumérica**]")
            df_filtrado = load_table(conexion, option, filtro_npn)
            if len(df_filtrado) == 0:
                st.markdown(":gray[*El Número Predial Nacional (NPN) no se encontró en la base alfanumérica*]")
            else:
                st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        st.markdown(":gray[**Información Alfanumérica**]")
        df_filtrado = load_table(conexion, option, filtro_npn)
        if len(df_filtrado) == 0:
                st.markdown(":gray[*El Número Predial Nacional (NPN) no se encontró en la base alfanumérica*]")
        else:
            st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
        st.sidebar.markdown(":gray[*El Número Predial Nacional (NPN) no se encontró en la base cartográfica*]")

elif option == 'COORDENADAS':
    option1 = 'ID_PREDIO'
    option2 = 'IDPREDIO'
    try:
        coordenadas = st.sidebar.text_input("COORDENADAS:", placeholder="3.4248559, -76.5188715")
        if coordenadas:
            coordenadas = coordenadas.split(',')
            latitud = float(coordenadas[0])
            longitud = float(coordenadas[1].strip())

            coordenadas = [(latitud, longitud)]
            df_point = pd.DataFrame([(latitud, longitud)], columns=['Latitud', 'Longitud'])
            df_point['geometry'] = df_point.apply(lambda row: Point(row['Longitud'], row['Latitud']), axis=1) # Convertir las coordenadas en objetos Point 
            gdf_point = gpd.GeoDataFrame(df_point, geometry='geometry') # Crear el GeoDataFrame
            gdf_point.set_crs(epsg=4326, inplace=True) # Establecer el sistema de referencia de coordenadas (CRS)           

            selected_gdf = load_predio_intersect(conexion, latitud, longitud)
            
            if int(selected_gdf['COMUNA'][0]) <= 22:
                vecinos = load_manzana(conexion, selected_gdf['CONEXION'][0][:-4])
            else:
                vecinos = load_vecino(conexion, option2, int(selected_gdf['IDPREDIO'][0]))

            m.add_gdf(vecinos, layer_name='Predios', zoom_to_layer=False, style={'color':'gray', 'fill':'gray', 'weight':1})
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m_streamlit = m.to_streamlit(800, 600)
            st.markdown(":gray[**Información Alfanumérica**]")
            df_filtrado = load_table(conexion, option1, int(selected_gdf['IDPREDIO']))
            st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_gdf['LATITUD'].values[0]},{selected_gdf['LONGITUD'].values[0]}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        st.sidebar.markdown(":gray[*No se encontró ningún predio en las coordenadas aportadas*]")