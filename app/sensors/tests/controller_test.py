from fastapi.testclient import TestClient
import pytest
import time
import json
from app.main import app
from shared.redis_client import RedisClient
from shared.mongodb_client import MongoDBClient
from shared.cassandra_client import CassandraClient
from shared.timescale import Timescale
from shared.elasticsearch_client import ElasticsearchClient

client = TestClient(app)

@pytest.fixture(scope="session", autouse=True)
def clear_dbs():
    from shared.database import engine
    from shared.sensors import models
    models.Base.metadata.drop_all(bind=engine)
    models.Base.metadata.create_all(bind=engine)
    redis = RedisClient(host="redis")
    redis.clearAll()
    redis.close()
    mongo = MongoDBClient(host="mongodb")
    mongo.clearDb("sensors")
    mongo.close()
    es = ElasticsearchClient(host="elasticsearch")
    es.clearIndex("sensors")  
    ts = Timescale()
    ts.execute("DROP TABLE IF EXISTS sensor_data")
    ts.close()

    while True:
        try:
            cassandra = CassandraClient(["cassandra"])
            cassandra.get_session().execute("DROP KEYSPACE IF EXISTS sensor")
            cassandra.close()
            break
        except Exception as e:
            time.sleep(5)

     
#TODO ADD all your tests in test_*.py files:
#CLAU_VALOR
def test_redis_connection():
    redis_client = RedisClient(host="redis")
    assert redis_client.ping()
    redis_client.close()
    
#DOCUMENTAL
def test_mongodb_connection():
    mongodb_client = MongoDBClient(host="mongodb")
    assert mongodb_client.ping()
    mongodb_client.close()


#CREATE
def test_create_sensor_temperatura_1():
    """A sensor can be properly created"""
    response = client.post("/sensors", json={"name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"})
    assert response.status_code == 200
    assert response.json() == {"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}

def test_post_sensor_data_not_exists():
    response = client.post("/sensors/4/data", json={"temperature": 1.0, "humidity": 1.0, "battery_level": 1.0, "last_seen": "2020-01-01T00:00:00.000Z"})
    assert response.status_code == 404
    assert "Sensor not found" in response.text

def test_get_sensor_data_not_exists():
    response = client.get("/sensors/4/data")
    assert response.status_code == 404
    assert "Sensor not found" in response.text

#SEARCH
def test_elasticsearch_client():
    """Elasticsearch client can be properly created"""
    es = ElasticsearchClient(host="elasticsearch")
    assert es.ping()
    # Create the index
    es_index_name='my_index'
    es.create_index(es_index_name)
    # Define the mapping for the index
    mapping = {
        'properties': {
            'title': {'type': 'text'},
            'description': {'type': 'text'},
            'price': {'type': 'float'}
        }
    }
    es.create_mapping('my_index',mapping)
    es_doc={
        'title': 'Test document',
        'description': 'This is a test document',
        'price': 10.0
    }
    es.index_document(es_index_name, es_doc)
    # Define the search query
    query = {
        'query': {
            'match': {
                'title': 'Test document'
            }
        }
    }
    # Perform the search and get the results
    results = es.search(es_index_name, query)

    # Loop through the results and print the title and price of each document
    for hit in results['hits']['hits']:
        print(f"Title: {hit['_source']['title']}")
        print(f"Price: {hit['_source']['price']}")
        assert hit['_source']['title'] == 'Test document'
        assert hit['_source']['price'] == 10.0
    
    # Delete the index
    es.clearIndex(es_index_name)
    es.close()
#CREATE
def test_create_sensor_velocitat_1():
    response = client.post("/sensors", json={"name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"})
    assert response.status_code == 200
    assert response.json() == {"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"}

def test_create_sensor_velocitat_2():
    response = client.post("/sensors", json={"name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"})
    assert response.status_code == 200
    assert response.json() == {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"}


def test_search_sensors_temperatura():
    time.sleep(10)
    """Sensors can be properly searched by type"""
    response = client.get('/sensors/search?query={"type":"Temperatura"}')
    assert response.status_code == 200
    assert response.json() == [{"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"}]

def test_search_sensors_name_similar():
    """Sensors can be properly searched by name"""
    response = client.get('/sensors/search?query={"name":"Velocidad 1"}&search_type=similar')
    assert response.status_code == 200
    assert response.json() == [{"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"}]

def test_search_sensors_name_prefix():
    """Sensors can be properly searched by name"""
    response = client.get('/sensors/search?query={"name":"Veloci"}&search_type=prefix')
    assert response.status_code == 200
    assert response.json() == [
        {"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"},
        {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model":"Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"}]
    
def test_search_sensors_type_limit():
    """Sensors can be properly searched by type"""
    response = client.get('/sensors/search?query={"type":"Velocitat"}&size=1')
    assert response.status_code == 200
    assert len(response.json()) == 1

def test_search_sensors_description_similar():
    time.sleep(5)
    """Sensors can be properly searched by description"""
    response = client.get('/sensors/search?query={"description":"dummy"}&search_type=similar')
    assert response.status_code == 200
    assert response.json() == [{"id": 1, "name": "Sensor Temperatura 1", "latitude": 1.0, "longitude": 1.0, "type": "Temperatura", "mac_address": "00:00:00:00:00:00", "manufacturer": "Dummy", "model":"Dummy Temp", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de temperatura model Dummy Temp del fabricant Dummy"},
        {"id": 2, "name": "Velocitat 1", "latitude": 1.0, "longitude": 1.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:01", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0000", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 1"},
        {"id": 3, "name": "Velocitat 2", "latitude": 2.0, "longitude": 2.0, "type": "Velocitat", "mac_address": "00:00:00:00:00:02", "manufacturer": "Dummy", "model": "Dummy Vel", "serie_number": "0000 0000 0000 0002", "firmware_version": "1.0", "description": "Sensor de velocitat model Dummy Vel del fabricant Dummy cruïlla 2"}] 
