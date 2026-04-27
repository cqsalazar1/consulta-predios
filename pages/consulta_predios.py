import folium
import psycopg2
import googlemaps
import pandas as pd
import streamlit as st
import geopandas as gpd
from shapely import wkt
from shapely.geometry import Point
import leafmap.foliumap as leafmap
from folium.plugins import MeasureControl, MousePosition
from folium import Element, MacroElement
from jinja2 import Template

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
    consulta = f"""SELECT * FROM "export_MAESTRO_predio_112025" WHERE "{option}" = '{input}' """
    cursor.execute(consulta)
    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(data, columns=columns)
    
    try:
        if df['ID_TERRENO'].values[0] is None:
            gdf = gpd.GeoDataFrame(df)
            gdf = gdf.drop(columns=['index'])
            return gdf
        else:
            id_terreno = df['ID_TERRENO'].values[0]
            consulta = f"""SELECT * FROM "export_MAESTRO_predio_112025" WHERE "ID_TERRENO" = '{id_terreno}' """
            cursor.execute(consulta)
            data = cursor.fetchall()
            columns = [col[0] for col in cursor.description]  # Obtener nombres de columnas
            df = pd.DataFrame(data, columns=columns)
            gdf = gpd.GeoDataFrame(df)
            gdf = gdf.drop(columns=['index'])
            return gdf
    except:
        cursor.execute(consulta)
        data = cursor.fetchall()
        columns = [col[0] for col in cursor.description]  # Obtener nombres de columnas
        df = pd.DataFrame(data, columns=columns)
        gdf = gpd.GeoDataFrame(df)
        gdf = gdf.drop(columns=['index'])
        return gdf

# Cargue de información alfanumérica - Solo para NOMBRE PROPIEDAD
@st.cache_data
def load_table2(_conexion, option, input):
    cursor = _conexion.cursor() # Crear un cursor para ejecutar consultas
    consulta = f"""SELECT * FROM "export_MAESTRO_predio_112025" WHERE "{option}" ILIKE '%{input}%' """
    cursor.execute(consulta)
    data = cursor.fetchall()
    columns = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(data, columns=columns)
    df = df.drop(columns=['index'])
    return df
    
# Cargue de información cartográfica
@st.cache_data
def load_predio(_conexion, option, input):
    cursor = _conexion.cursor() # Crear un cursor para ejecutar consultas
    consulta = f""" SELECT *, ST_AsText(geometry) AS wkt FROM terrenos WHERE "{option}" IN ('{input}') """

    cursor.execute(consulta)

    columnas = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(cursor.fetchall(), columns=columnas)
    df['wkt'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='wkt', crs='4326')
    #gdf = gdf.drop(columns=['Shape_Leng', 'geometry'])
    gdf = gdf[[col for col in gdf.columns if col != 'geometry' and col != 'SHAPE_Leng']]
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
    #gdf = gdf.drop(columns=['Shape_Leng', 'geometry'])
    gdf = gdf[[col for col in gdf.columns if col != 'geometry' and col != 'SHAPE_Leng']]
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
    #gdf = gdf.drop(columns=['Shape_Leng', 'LATITUD', 'LONGITUD', 'geometry'])
    gdf = gdf[[col for col in gdf.columns if col != 'geometry' and col != 'SHAPE_Leng']]
    return gdf

@st.cache_data
def load_vecino(_conexion, option, input):
    cursor = _conexion.cursor() # Crear un cursor para ejecutar consultas
    consulta = f""" SELECT *, ST_AsText(geometry) AS wkt FROM terrenos WHERE ST_Touches(geometry, (SELECT geometry FROM terrenos WHERE "{option}" = '{input}' LIMIT 1)) """
    cursor.execute(consulta)
    
    columnas = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(cursor.fetchall(), columns=columnas)
    df['wkt'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='wkt', crs='4326')
    #gdf = gdf.drop(columns=['Shape_Leng', 'LATITUD', 'LONGITUD', 'geometry'])
    gdf = gdf[[col for col in gdf.columns if col != 'geometry' and col != 'SHAPE_Leng']]
    return gdf

@st.cache_data
def load_capa(_conexion, capa):
    cursor = _conexion.cursor() # Crear un cursor para ejecutar consultas
    consulta = f""" SELECT *, ST_AsText(geometry) AS wkt FROM {capa} """
    cursor.execute(consulta)
    columnas = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(cursor.fetchall(), columns=columnas)
    df['wkt'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='wkt', crs='4326')
    gdf = gdf.drop(columns=['geometry'])
    return gdf

def direccion_parametrizada():
    calle = st.sidebar.selectbox("Seleccione el tipo de vía principal", ("Calle", "Carrera", "Avenida", "Transversal", "Diagonal"))
    numero = st.sidebar.text_input("Número de la vía principal", placeholder="Ejemplo: 12")
    letra_complemento = st.sidebar.text_input("Letra y complemento de la vía principal", placeholder="Ejemplos: A, A1, BIS, A1 BIS, Oeste")
    numero_secundario = st.sidebar.text_input("Número secundario", placeholder="Ejemplo: 12")
    letra_complemento_secundario = st.sidebar.text_input("Letra y complemento del número secundario", placeholder="Ejemplos: A, A1, BIS, A1 BIS, Oeste")
    numero_placa = st.sidebar.text_input("Número de la placa", placeholder="Ejemplo: 12")

    direccion = f"{calle} {numero}{letra_complemento} #{numero_secundario}{letra_complemento_secundario}-{numero_placa}"
    direccion_completa = f"{direccion}, Cali, Colombia"
    st.sidebar.markdown("**:gray[Dirección parametrizada:]**")
    st.sidebar.markdown(f"**:gray-background[{direccion}]**")
                        
    if calle and numero and numero_secundario and numero_placa:
        if st.sidebar.button("Espacializar dirección", use_container_width=True):
            return direccion_completa

def geocode_address(address, api_key):
    # Inicializar cliente de Google Maps
    gmaps = googlemaps.Client(key=api_key)
    
    # Realizar la geocodificación
    geocode_result = gmaps.geocode(address)
    
    if geocode_result:
        # Extraer latitud y longitud del primer resultado
        location = geocode_result[0]['geometry']['location']
        lat = location['lat']
        lng = location['lng']
        formatted_address = geocode_result[0]['formatted_address']
        return {
            'address': formatted_address,
            'latitude': lat,
            'longitude': lng
        }
    else:
        return None

@st.cache_data
def load_zonas(_conexion, id_terreno):
    cursor = _conexion.cursor() # Crear un cursor para ejecutar consultas
    consulta = f""" SELECT *, ST_AsText(geometry) AS wkt FROM zonas_homogeneas WHERE ST_Intersects(geometry, (SELECT geometry FROM terrenos WHERE "CONEXION" = '{id_terreno}' LIMIT 1)) """
    cursor.execute(consulta)
    
    columnas = [col[0] for col in cursor.description]  # Obtener nombres de columnas
    df = pd.DataFrame(cursor.fetchall(), columns=columnas)
    df['wkt'] = df['wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='wkt', crs='4326')
    gdf = gdf[[col for col in gdf.columns if col != 'geometry' and col != 'SHAPE_Leng']]
    return gdf

st.set_page_config(page_title='Consulta de Predios', layout='centered', page_icon="🌎")

st.subheader("Consulta Predial", divider='gray')

m = leafmap.Map(
    tiles='Cartodb Positron',
    #google_map='ROADMAP',
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

m.add_child(MeasureControl(position='bottomleft'))
m.add_basemap(basemap="HYBRID", show=False)
m.add_basemap(basemap="ROADMAP", show=False)

MousePosition().add_to(m)

m.add_child(
    folium.LatLngPopup()
)

conexion = conectar_bd()

m.add_gdf(load_capa(conexion, 'barrios'), layer_name='Barrios', zoom_to_layer=False, style={'color':"#EEB928", 'fill':'white', 'fillOpacity':0.1, 'weight':1.5}, show=False)
m.add_gdf(load_capa(conexion, 'comunas'), layer_name='Comunas', zoom_to_layer=False, style={'color':"#7C1414", 'fill':'white', 'fillOpacity':0.05, 'weight':2}, show=False)
m.add_gdf(load_capa(conexion, 'corregimientos'), layer_name='Corregimientos', zoom_to_layer=False, style={'color':"#5F3B13", 'fill':'white', 'fillOpacity':0.1, 'weight':2}, show=False)

# Carga de servicios WMS
wms_url = "https://ws-idesc.cali.gov.co/geoserver/wms?service=WMS&"

# Datos de capas y leyendas
capas = [
    {
        "layer_name": "pot_2014:nur_areas_actividad",
        "display_name": "Areas de actividad",
        "legend_pos": "left: 495px;",
        "div_id": "legend_areas_actividad"
    },
    {
        "layer_name": "pot_2014:nur_tratamientos_urbanisticos",
        "display_name": "Tratamientos urbanisticos",
        "legend_pos": "left: 412px;",
        "div_id": "legend_tratamientos_urbanisticos"
    },
    {
        "layer_name": "pot_2014:nru_areas_manejo_rural",
        "display_name": "Areas de manejo rural",
        "legend_pos": "left: 380px;",
        "div_id": "legend_areas_manejo_rural"
    }
]

# Agregar capas y leyendas
for capa in capas:
    # Agregar WMS layer
    m.add_wms_layer(
        url=wms_url,
        layers=capa["layer_name"],
        name=capa["display_name"],
        shown=False
    )
    
    # Crear HTML para leyenda
    legend_url = f'{wms_url}SERVICE=WMS&REQUEST=GetLegendGraphic&FORMAT=image/png&LAYER={capa["layer_name"]}'
    legend_html = f"""
    <div id="{capa['div_id']}" style="
        position: fixed;
        bottom: 30px;
        {capa['legend_pos']}
        z-index: 800;
        border: 1px solid black;
        padding: 10px;
        background-color: white;
        border-radius: 5px;
        box-shadow: 0 0 10px rgba(0,0,0,0.3);
        display: none;
        pointer-events: none;">
      <img src="{legend_url}" alt="Leyenda {capa['display_name']}" style="max-width: 300px;">
    </div>
    """
    m.get_root().html.add_child(Element(legend_html))

# Clase general para vincular capas con leyendas
class BindLegend(MacroElement):
    def __init__(self, layer_name_to_divid):
        super().__init__()
        # Recibe lista tuplas (nombre_capa, id_leyenda_div)
        self.layer_div_mappings = layer_name_to_divid
        # Plantilla para manejar múltiples capas y leyendas al mismo tiempo
        self._template = Template(u"""
        {% macro script(this, kwargs) %}
            function setLegendsVisibility() {
                var checkboxes = document.querySelectorAll('input.leaflet-control-layers-selector');
                {% for layer_name, div_id in this.layer_div_mappings %}
                var legend{{loop.index}} = document.getElementById("{{div_id}}");
                var active{{loop.index}} = false;
                checkboxes.forEach(cb => {
                    const label = cb.nextSibling.textContent.trim();
                    if(label === "{{layer_name}}" && cb.checked) active{{loop.index}} = true;
                });
                if (legend{{loop.index}}) {
                    legend{{loop.index}}.style.display = active{{loop.index}} ? 'block' : 'none';
                }
                {% endfor %}
            }

            document.addEventListener('DOMContentLoaded', () => {
                setLegendsVisibility();
                var checkboxes = document.querySelectorAll('input.leaflet-control-layers-selector');
                checkboxes.forEach(cb => cb.addEventListener('change', setLegendsVisibility));
            });
        {% endmacro %}
        """)

# Generar lista de mapeos con nombres visibles y divs leyendas
layer_div_list = [(capa["display_name"], capa["div_id"]) for capa in capas]

bind_legend = BindLegend(layer_div_list)
m.get_root().add_child(bind_legend)

url_ortofotos = "http://172.18.21.78:7070/geoserver/ortofoto/wms?"
m.add_wms_layer(
    url=url_ortofotos,
    name="Ortofoto Urbana",
    layers="ortofoto_urbano_2023",
    shown=False,
)
m.add_wms_layer(
    url=url_ortofotos,
    name="Ortofoto Rural",
    layers="ortofot_rural_2023",
    shown=False,
)

## CONSULTAS
option = st.sidebar.selectbox(
    "Seleccione el tipo de consulta",
    ("ID PREDIO", "ID TERRENO", "COORDENADAS", "NOMBRE PROPIEDAD", "DIRECCIÓN")
)

if option == 'ID PREDIO':
    option1 = 'ID_PREDIO'
    option2 = 'CONEXION'

    try:
        filtro_id_predio = st.sidebar.number_input("ID PREDIO:", value=None, min_value=0, placeholder=0)
        selected_df = load_table(conexion, option1, filtro_id_predio)
        if len(selected_df) == 0:
            st.sidebar.markdown(":gray[*El ID PREDIO no se encontró en la base alfanumérica*]")
            m_streamlit = m.to_streamlit(800, 600)
        else:
            id_terreno = selected_df['ID_TERRENO'][0]
            selected_gdf = load_predio(conexion, option2, id_terreno)
            try:
                if selected_gdf['CONEXION'][0] is None:
                    vecinos = load_vecino(conexion, option2, id_terreno)
                elif selected_gdf['CONEXION'][0] is not None and int(selected_gdf['COMUNA'][0]) <= 22:
                    vecinos = load_manzana(conexion, selected_gdf['CONEXION'][0][:-4])
                else:
                    vecinos = load_vecino(conexion, option2, selected_df['ID_TERRENO'][0])
                m.add_gdf(vecinos, layer_name='Predios', zoom_to_layer=False, style={'color':'gray', 'fill':'gray', 'weight':1})
            except:
                pass
            
            m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
            m.add_gdf(load_zonas(conexion, selected_gdf['CONEXION'][0]), layer_name='Zonas homogeneas', zoom_to_layer=False, style={'color':"#7023AF", 'fill':'white', 'fillOpacity':0.1, 'weight':0.5}, show=False)                 
            st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_df['LATITUD'].values[0]},{selected_df['LONGITUD'].values[0]}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
            m_streamlit = m.to_streamlit(800, 600)
            st.markdown(":gray[**Información Alfanumérica**]")
            st.data_editor(selected_df, key="my_key", num_rows="fixed")
        
    except:
        m_streamlit = m.to_streamlit(800, 600)

if option == 'ID TERRENO':
    option1 = 'CONEXION'
    option2 = 'ID_TERRENO'
    
    try:
        filtro_id_terreno = st.sidebar.text_input("ID TERRENO:", placeholder='190600030002')
        if filtro_id_terreno:
            selected_df = load_table(conexion, option2, filtro_id_terreno)
            if len(selected_df) == 0:
                st.sidebar.markdown(":gray[*El ID TERRENO no se encontró en la base alfanumérica*]")
                m_streamlit = m.to_streamlit(800, 600)
            else:
                id_terreno = selected_df['ID_TERRENO'][0]
                selected_gdf = load_predio(conexion, option1, id_terreno)
                try:
                    if selected_gdf['CONEXION'][0] is None:
                        vecinos = load_vecino(conexion, option1, id_terreno)
                    elif selected_gdf['CONEXION'][0] is not None and int(selected_gdf['COMUNA'][0]) <= 22:
                        vecinos = load_manzana(conexion, selected_gdf['CONEXION'][0][:-4])
                    else:
                        vecinos = load_vecino(conexion, option1, selected_df['ID_TERRENO'][0])
                    m.add_gdf(vecinos, layer_name='Predios', zoom_to_layer=False, style={'color':'gray', 'fill':'gray', 'weight':1})
                except:
                    pass
            
                m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
                m.add_gdf(load_zonas(conexion, selected_gdf['CONEXION'][0]), layer_name='Zonas homogeneas', zoom_to_layer=False, style={'color':"#7023AF", 'fill':'white', 'fillOpacity':0.1, 'weight':0.5}, show=False)                 
                st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={selected_df['LATITUD'].values[0]},{selected_df['LONGITUD'].values[0]}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
                m_streamlit = m.to_streamlit(800, 600)
                st.markdown(":gray[**Información Alfanumérica**]")
                st.data_editor(selected_df, key="my_key", num_rows="fixed")  
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)

elif option == 'COORDENADAS':
    option1 = 'ID_TERRENO'

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

            try:
                if selected_gdf['CONEXION'][0] is None:
                    vecinos = load_vecino(conexion, option1, selected_df['ID_TERRENO'][0])
                elif selected_gdf['CONEXION'][0] is not None and int(selected_gdf['COMUNA'][0]) <= 22:
                    vecinos = load_manzana(conexion, selected_gdf['CONEXION'][0][:-4])
                else:
                    vecinos = load_vecino(conexion, option1, selected_df['ID_TERRENO'][0])
                m.add_gdf(vecinos, layer_name='Predios', zoom_to_layer=False, style={'color':'gray', 'fill':'gray', 'weight':1})
            except:
                pass

            try:
                m.add_marker(location=[latitud, longitud],
                popup=f"Latitud: {round(latitud,5)}\n Longitud: {round(longitud,5)}",
                icon=folium.Icon(color="green", icon='screenshot'))
                m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
                m.add_gdf(load_zonas(conexion, selected_gdf['CONEXION'][0]), layer_name='Zonas homogeneas', zoom_to_layer=False, style={'color':"#7023AF", 'fill':'white', 'fillOpacity':0.1, 'weight':0.5}, show=False)                 
                m_streamlit = m.to_streamlit(800, 600)
                st.markdown(":gray[**Información Alfanumérica**]")
                selected_df = load_table(conexion, option1, selected_gdf['CONEXION'][0])
                st.data_editor(selected_df, key="my_key", num_rows="fixed")
                st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={latitud},{longitud}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
            except:
                try:
                    m.add_marker(location=[latitud, longitud],
                        popup=f"Latitud: {round(latitud,5)}\n Longitud: {round(longitud,5)}",
                        icon=folium.Icon(color="red", icon='question-sign'))
                    m.set_center(longitud, latitud, zoom=19)
                    st.sidebar.markdown(":gray[*No se encontró ningún predio en las coordenadas aportadas*]")
                    m_streamlit = m.to_streamlit(800, 600)
                except NameError as e:
                    st.sidebar.markdown(f":gray[*{e}*]")
                    m_streamlit = m.to_streamlit(800, 600)
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        try:
            m.add_marker(location=[latitud, longitud],
            popup=f"Latitud: {round(latitud,5)}\n Longitud: {round(longitud,5)}",
            icon=folium.Icon(color="red", icon='question-sign'))
            m.set_center(longitud, latitud, zoom=19)
            st.sidebar.markdown(":gray[*No se encontró ningún predio en las coordenadas aportadas*]")
            m_streamlit = m.to_streamlit(800, 600)
        except NameError as e:
            st.sidebar.markdown(f":gray[*{e}*]")
            m_streamlit = m.to_streamlit(800, 600)

elif option == 'NOMBRE PROPIEDAD':
    option1 = 'NOMBRE_EDIFICIO'
    option2 = 'CONEXION'
    try:
        filtro_nom_propiedad = st.sidebar.text_input("NOMBRE PROPIEDAD:", placeholder='Ejemplo: TORRES DE TEQUENDAMA')
        if filtro_nom_propiedad:
            select_df = load_table2(conexion, option1, filtro_nom_propiedad)
            num_filas = select_df.shape[0]
            id_terreno = select_df['ID_TERRENO'].unique().tolist()
            predios = []
            for predio in id_terreno:
                predio = f'{str(predio)}'
                predios.append(predio)
            predios = "', '".join(map(str,predios))   #Convertir lista a string separando por coma y agregando comillas simples
            selected_gdf = load_predio(conexion, option2, predios)
            num_terrenos = selected_gdf.shape[0]

            if len(selected_gdf) == 0:
                st.sidebar.markdown(":gray[*La PROPIEDAD no se encontró en la base cartográfica*]")
                m_streamlit = m.to_streamlit(800, 600)
            if num_filas == 0:
                st.markdown(":gray[**Información Alfanumérica**]")
                st.markdown(":gray[*La PROPIEDAD no se encontró en la base alfanumérica*]")
            else:
                if num_terrenos == 1:
                    try:    
                        if selected_gdf['CONEXION'][0] is None:
                            vecinos = load_vecino(conexion, option1, filtro_id_predio)
                        elif selected_gdf['CONEXION'][0] is not None and int(selected_gdf['COMUNA'][0]) <= 22:
                            vecinos = load_manzana(conexion, selected_gdf['CONEXION'][0][:-4])
                        else:
                            vecinos = load_vecino(conexion, option1, filtro_id_predio)
                        m.add_gdf(vecinos, layer_name='Predios', zoom_to_layer=False, style={'color':'gray', 'fill':'gray', 'weight':1})
                    except:
                        pass            
                    st.sidebar.markdown(f":gray[*Terrenos encontrados: {num_terrenos}*]")
                    st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={select_df['LATITUD'].values[0]},{select_df['LONGITUD'].values[0]}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
                    m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
                    m_streamlit = m.to_streamlit(800, 600)
                    st.markdown(":gray[**Información Alfanumérica**]")
                    st.markdown(f":gray[*Registros encontrados: {num_filas}*]")
                    st.data_editor(select_df, key="my_key", num_rows="fixed")
                else:
                    m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
                    m_streamlit = m.to_streamlit(800, 600)
                    st.sidebar.markdown(f":gray[*Terrenos encontrados: {num_terrenos}*]")
                    st.markdown(":gray[**Información Alfanumérica**]")
                    st.markdown(f":gray[*Registros encontrados: {num_filas}*]")
                    st.data_editor(select_df, key="my_key", num_rows="fixed")
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)

if option == 'DIRECCIÓN':
    option1 = 'ID_TERRENO'
    option2 = 'IDPREDIO'
    direccion = ""
    try:
        direccion = direccion_parametrizada()
        if direccion:
            API_KEY = st.secrets['GOOGLE_MAPS_API_KEY']
            
            resultado = geocode_address(direccion, API_KEY)
                   
            latitud = resultado['latitude']
            longitud = resultado['longitude']

            coordenadas = [(latitud, longitud,5)]
            df_point = pd.DataFrame([(latitud, longitud)], columns=['Latitud', 'Longitud'])
            df_point['geometry'] = df_point.apply(lambda row: Point(row['Longitud'], row['Latitud']), axis=1) # Convertir las coordenadas en objetos Point 
            gdf_point = gpd.GeoDataFrame(df_point, geometry='geometry') # Crear el GeoDataFrame
            gdf_point.set_crs(epsg=4326, inplace=True) # Establecer el sistema de referencia de coordenadas (CRS)           

            try:
                selected_gdf = load_predio_intersect(conexion, latitud, longitud)
                           
                if selected_gdf['CONEXION'][0] is None:
                    vecinos = load_vecino(conexion, option2, int(selected_gdf['IDPREDIO'][0]))
                elif selected_gdf['CONEXION'][0] is not None and int(selected_gdf['COMUNA'][0]) <= 22:
                    vecinos = load_manzana(conexion, selected_gdf['CONEXION'][0][:-4])
                else:
                    vecinos = load_vecino(conexion, option2, int(selected_gdf['IDPREDIO'][0]))

                m.add_marker(location=[latitud, longitud],
                            popup=f"Latitud: {round(latitud,5)}\n Longitud: {round(longitud,5)}",
                            icon=folium.Icon(color="green", icon='screenshot'))
                m.add_gdf(vecinos, layer_name='Predios', zoom_to_layer=False, style={'color':'gray', 'fill':'gray', 'weight':1})
                m.add_gdf(selected_gdf, layer_name='Predio seleccionado', zoom_to_layer=True, style={'color':'red', 'fill':'red', 'weight':2})
                m_streamlit = m.to_streamlit(800, 600)
                st.markdown(":gray[**Información Alfanumérica**]")
                df_filtrado = load_table(conexion, option1, selected_gdf['CONEXION'][0])
                if len(df_filtrado) == 0:
                    st.markdown(":gray[*No se encontró ningún predio en la base alfanumérica*]")
                else:
                    st.data_editor(df_filtrado, key="my_key", num_rows="fixed")
                st.sidebar.link_button('Google Maps', f"https://maps.google.com/?q={latitud},{longitud}", type='tertiary', icon=":material/pin_drop:", use_container_width=True)
            except:
                m.add_marker(location=[latitud, longitud],
                            popup=f"Latitud: {round(latitud,5)}\n Longitud: {round(longitud,5)}",
                            icon=folium.Icon(color="red", icon='question-sign'))
                m.set_center(longitud, latitud, zoom=19)
                
                m_streamlit = m.to_streamlit(800, 600)
                st.sidebar.markdown(":gray[*No se encontró ningún predio en la dirección aportada*]")
        else:
            m_streamlit = m.to_streamlit(800, 600)
    except:
        m_streamlit = m.to_streamlit(800, 600)
        st.sidebar.markdown(":gray[*No se encontró ningún predio en la dirección aportada*]")
