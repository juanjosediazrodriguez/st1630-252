import os

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SUPERSET_WEBSERVER_TIMEOUT = 120
# Sin caché: la cifra del tablero debe igualar la consulta directa a Gold (RF-11)
# y las ventas por minuto deben verse en <= 2 min (RF-09).
DATA_CACHE_CONFIG = {"CACHE_TYPE": "NullCache"}
