import yaml
import streamlit as st
from yaml.loader import SafeLoader
import streamlit_authenticator as stauth
from streamlit_authenticator.utilities import LoginError

st.set_page_config(page_title='Consulta de Predios', layout='centered', page_icon="🔍")

def hide_sidebar():
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] {
            visibility: hidden;
        }
        </style>
        """,
        unsafe_allow_html=True
    )

hide_sidebar()

# Load credentials from the YAML file
with open("config.yaml") as file:
     config = yaml.load(file, Loader=SafeLoader)

authenticator = stauth.Authenticate(
    config["credentials"],
    config["cookie"]["name"],
    config["cookie"]["key"],
    config["cookie"]["expiry_days"],
)

# Store the authenticator object in the session state
st.session_state["authenticator"] = authenticator
# Store the config in the session state so it can be updated later
st.session_state["config"] = config

# Authentication logic
try:
    authenticator.login(location="main", key="login-demo-app-home")
except LoginError as e:
    st.error(e)

if st.session_state["authentication_status"]:
    st.switch_page('pages/consulta_predios.py')
elif st.session_state["authentication_status"] is False:
    st.error("Usuario o contraseña incorrecto")
elif st.session_state["authentication_status"] is None:
    st.warning("Por favor ingrese su usuario y contraseña")

