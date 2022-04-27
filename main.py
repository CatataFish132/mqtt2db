import sqlite3
import paho.mqtt.client as mqtt
import json

def main():
    client = mqtt.Client(client_id="PI")
    client.username_pw_set("luni", "12345")
    client.on_message=on_message
    client.connect("172.21.1.79")
    con = sqlite3.connect('databasepi')
    cur = con.cursor()
    print("test")
    client.subscribe("data")
    client.loop_forever()

    
    # sql_statement = ""
    # cur.execute(sql_statement)
    # con.commit()

def on_message(client, userdata, message):
    data = json.loads(message.payload)
    print(dir(message))
    sql_statement = f"INSERT INTO datapi (device_id, temperature, humidity, pressure, timestamp, datetime) VALUES (0, {data['temperature']}, {data['humidity']}, {data['pressure']}, {message.ts}, '2007-01-01 10:00:00')"

if __name__ == "__main__":
    main()
