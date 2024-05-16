from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime
from bson.son import SON

from shared.mongodb_client import MongoDBClient
from shared.redis_client import RedisClient
from shared.sensors import models, schemas
from shared.timescale import Timescale
from shared.cassandra_client import CassandraClient


class DataCommand():
    def __init__(self, from_time, to_time, bucket):
        if not from_time or not to_time:
            raise ValueError("from_time and to_time must be provided")
        if not bucket:
            bucket = 'day'
        self.from_time = from_time
        self.to_time = to_time
        self.bucket = bucket


def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()


def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()


def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient, elasticsearch_client: ElasticsearchClient, cassandra_client: CassandraClient) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude, type=sensor.type, mac_address=sensor.mac_address, manufacturer=sensor.manufacturer, model=sensor.model, serie_number=sensor.serie_number, firmware_version=sensor.firmware_version, description=sensor.description)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    mongodb_client.insertDoc(sensor.dict())
    
    #Afegim el name, type i description
    data = {
        'name': sensor.name,
        'type': sensor.type,
        'description': sensor.description
    }
    elasticsearch_client.index_document(index_name="sensors", document=data)
    sensor_dict = sensor.dict()
    sensor_dict.update({'id': db_sensor.id})

    #Guardamos el id y el type en la tabla quantity
    query_increment = f"INSERT INTO sensor.quantity(id, type) VALUES ({db_sensor.id}, '{sensor.type}');"
    cassandra_client.execute(query_increment)
    return sensor_dict


def add_sensor_to_postgres(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    date = datetime.now()

    db_sensor = models.Sensor(name=sensor.name, joined_at=date)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)

    return db_sensor


def add_sensor_to_mongodb(mongodb_client: MongoDBClient, db_sensor: schemas.SensorCreate, id):
    mongo_projection = schemas.SensorMongoProjection(id=id, name=db_sensor.name, location={'type': 'Point',
                                                                                           'coordinates': [
                                                                                               db_sensor.longitude,
                                                                                               db_sensor.latitude]},
                                                     type=db_sensor.type, mac_address=db_sensor.mac_address,
                                                     description=db_sensor.description,
                                                     serie_number=db_sensor.serie_number,
                                                     firmware_version=db_sensor.firmware_version, model=db_sensor.model,
                                                     manufacturer=db_sensor.manufacturer)
    mongodb_client.getDatabase()
    mongoInsert = mongo_projection.dict()
    mongodb_client.getCollection().insert_one(mongoInsert)
    return mongo_projection.dict()


#Modificat: Creem la funcio record_data
#Volem que els sensors puguin escriure les seves dades a la base
def record_data(redis: RedisClient, sensor_id: int, data: schemas.SensorData, timescale: Timescale, cassandra_client: CassandraClient) -> schemas.SensorData:
    
    #Cogemos los datos de data y lo pasamos a un diccionario
    data_sensor = data.dict()

    #Tenemos que transformar los datos que sean None a Null para poderlas poner en SQL
    new_data = {key: ('NULL' if value is None else value if key != 'last_seen' else f"'{value}'") for key, value in data_sensor.items()}

    #Creamos la query para añadir los datos a TimescaleDB
    query_sin_parametros = "INSERT INTO sensor_data (id, temperature, humidity, velocity, battery_level, last_seen) VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (id, last_seen) DO UPDATE SET temperature = EXCLUDED.temperature, humidity = EXCLUDED.humidity, velocity = EXCLUDED.velocity, battery_level = EXCLUDED.battery_level;"
    query = query_sin_parametros % (sensor_id, new_data['temperature'], new_data['humidity'], new_data['velocity'], new_data['battery_level'], new_data['last_seen'])
    timescale.execute(query)
    timescale.execute("commit")

    #Guardamos la temperatura
    if data.temperature is not None:
        query = f"INSERT INTO sensor.temperature(id, temperature) VALUES ({sensor_id}, {data.temperature});"
        cassandra_client.execute(query)

    #Guardamos la battery
    cassandra_client.execute(f"INSERT INTO sensor.battery(id, battery_level) VALUES ({sensor_id}, {data.battery_level});")

    redis.set(sensor_id, json.dumps(data_sensor))
    return json.loads(redis.get(sensor_id))


def getView(bucket: str) -> str:
    if bucket == 'year':
        return 'sensor_data_yearly'
    if bucket == 'month':
        return 'sensor_data_monthly'
    if bucket == 'week':
        return 'sensor_data_weekly'
    if bucket == 'day':
        return 'sensor_data_daily'
    elif bucket == 'hour':
        return 'sensor_data_hourly'
    else:
        raise ValueError("Invalid bucket size")


def get_data(redis: RedisClient, sensor_id: int, db: Session) -> schemas.Sensor:
    #Agafem la base de dades del sensor
    db_sensor = redis.get(sensor_id)
        
    #Si la tenim, retornem les dades més el id i el nom 
    if db_sensor:
        db_dada = json.loads(db_sensor)
        db_dada["id"] = sensor_id
        db_dada["name"] = get_sensor(db, sensor_id).name
        return db_dada

def get_data_timescale(sensor_id: int, timescale: Timescale, from_date: str, end_date: str, bucket: str) -> schemas.Sensor:
    # Contruimos la query para que nos de los datos de los sensores agrupados por intervalos de tiempo

    query = f"""
        SELECT 
            id,
            time_bucket('1 {bucket}', last_seen) AS {bucket},
            AVG(velocity) AS velocity,
            AVG(temperature) AS temperature,
            AVG(humidity) AS humidity
        FROM sensor_data
        WHERE id = {sensor_id} AND last_seen >= '{from_date}' AND last_seen <= '{end_date}'
        GROUP BY id, time_bucket('1 {bucket}', last_seen)
        ORDER BY {bucket} ASC;
    """

    # Ejecutamos la query
    timescale.execute(query)

    # Obtenemos los resultados
    result = timescale.getCursor().fetchall()

    return result

def delete_sensor(db: Session, sensor_id: int, mongodb_client: MongoDBClient, redis: RedisClient):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()

    #Eliminem de mongodb el fixer amb id sensor_id
    mongodb_client.deleteOne(sensor_id)

    #Eliminem el de redis també
    redis.delete(sensor_id)

    return db_sensor

def get_sensors_near(redis: RedisClient, mongodb_client: MongoDBClient, db: Session, latitude: float, longitude: float, radius: float):
    #Llista de retorn amb els nears que hi ha
    nears = []
    
    # Creem una query per a que busci aquells valors de latitude i longitude dins del radi rebut
    query = SON([
        ("latitude", SON([
            ("$gte", latitude - radius),
            ("$lte", latitude + radius)
        ])),
        ("longitude", SON([
            ("$gte", longitude - radius),
            ("$lte", longitude + radius)
        ]))
    ])
    
    # Recuperem tots els documents de sensors de la base de dades
    documents = mongodb_client.collection.find(query)

    for document in documents:
        sensor = get_sensor_by_name(db, document["name"])
        sensor_data = get_data(redis, sensor.id, db)
        data = {
            "id": sensor.id,
            "name": sensor.name
        }
        #Actualitzem les dades del sensor
        data.update(sensor_data)

        #Un cop ho tenim, ens guardem el sensor a la llista dels sensors nears
        nears.append(data)

    return nears

#Cerca de sensors
def search_sensors(db: Session, mongodb_client: MongoDBClient, query: str, size: int, search_type: str, elastic_client: ElasticsearchClient):
    search = list()

    if search_type == "similar":
        search_type = "fuzzy"
    
    search_query = {
        "query": {
            search_type: json.loads(query)
        }
    }

    # Perform the search and get the results
    results = elastic_client.search(index_name="sensors", query=search_query)

    # Loop through the results and print the title and price of each document
    for hit in results['hits']['hits']:
        # Agafem el name
        name = hit["_source"]["name"]
        sensor = get_sensor_by_name(db, name)
        search.append(sensor)
        # Tenim un size max
        if len(search) == size:
            break
    return search

def get_sensor_mongo(mongodb_client: MongoDBClient, db_sensor: models.Sensor) -> schemas.SensorCreate:
    sensor = mongodb_client.getDocument({'name': db_sensor.name})
    sensor.update({'id': db_sensor.id})
    return sensor

#Hemos de retornar un dict, ya que es lo que esperan los test
def get_temperature_values(db: Session, mongodb_client: MongoDBClient, cassandra_client: CassandraClient):
    
    query = """
        SELECT id, AVG(temperature) AS avg_temp, MIN(temperature) AS min_temp, MAX(temperature) AS max_temp
        FROM sensor.temperature 
        GROUP BY id;
        """
    results = cassandra_client.execute(query)

    sensors = []
    for row in results:
        db_sensor = get_sensor(db, row.id)
        sensor = get_sensor_mongo(mongodb_client, db_sensor)
        sensor["values"] = {"max_temperature": row.max_temp, "min_temperature": row.min_temp, "average_temperature": row.avg_temp}

        sensors.append(sensor)

    return {'sensors': sensors}

def get_sensors_quantity(cassandra_client: CassandraClient):
    query = """
        SELECT type, count(*) AS quantity FROM sensor.quantity GROUP BY type;
        """
    results = cassandra_client.execute(query)

    sensors = [{'type': row.type, 'quantity': row.quantity} for row in results]
    return {'sensors': sensors}

def get_low_battery_sensors(db: Session, mongodb_client: MongoDBClient, cassandra_client: CassandraClient):
    query = """
        SELECT id, battery_level FROM sensor.battery WHERE battery_level < 0.2 ALLOW FILTERING;
        """
    results = cassandra_client.execute(query)

    sensors = []
    for row in results:
        db_sensor = get_sensor(db, row.id)
        sensor = get_sensor_mongo(mongodb_client, db_sensor)
        sensor.update({"battery_level":  round(row.battery_level, 2)})

        sensors.append(sensor)
    return {'sensors': sensors}
    
