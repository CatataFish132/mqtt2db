import sqlite3
import paho.mqtt.client as mqtt
import json
from datetime import datetime
    
    # sql_statement = ""
    # cur.execute(sql_statement)
    # con.commit()

def on_message(client, userdata, message):
    data = json.loads(message.payload)
    print(dir(message))
    date_now = datetime.now()
    time_formatted = date_now.strftime("%A %B %d, %Y, %H:%M:%S")
    sql_statement = f"INSERT INTO data (device_id, temperature, humidity, pressure, rpi_datetime, sensor_datetime) VALUES (0, {data['temperature']}, {data['humidity']}, {data['pressure']}, '{time_formatted}', '{data['timestamp']}')"
    print(sql_statement)
    cur.execute(sql_statement)
    con.commit()
    

def create_table(client):
    sql_statement = "" 


if __name__ == "__main__":
    client = mqtt.Client(client_id="PI")
    client.username_pw_set("luni", "12345")
    client.on_message=on_message
    client.connect("127.0.0.1")
    con = sqlite3.connect('databasepi')
    cur = con.cursor()
    client.subscribe("data")
    client.loop_forever()