import fastapi
from .sensors.controller import router as sensorsRouter
import yoyo
import os

app = fastapi.FastAPI(title="Senser", version="0.1.0-alpha.1")

#TODO: Apply new TS migrations using Yoyo
#Read docs: https://ollycope.com/software/yoyo/latest/

app.include_router(sensorsRouter)

# Establecemos la conexión con la base de datos PostgreSQL de TimescaleDB
backend = yoyo.get_backend("postgresql://timescale:timescale@timescale:5433/timescale")

# Cogemos el path donde esta el directorio "migrations_ts" y leemos las migraciones
path = os.path.dirname(os.path.realpath('migrations_ts/migrations_ts.sql'))
migrations = yoyo.read_migrations(path)

# Bloqueamos para garantizar la aplicación segura de las migraciones
with backend.lock():
    # Aplicamos las migraciones pendinetes a la base de datos
    # Primero cogiendo las pendientes y despues aplicandoselo una por una en orden
    backend.apply_migrations(backend.to_apply(migrations))

@app.get("/")
def index():
    #Return the api name and version
    return {"name": app.title, "version": app.version}
