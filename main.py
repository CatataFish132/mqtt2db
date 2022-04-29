#!/usr/bin/env python#/bin/env/python
import sqlite3
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import app as IOT
import threading
import time

LAST_ID = None

def iot_callback(*args, **kwargs):
    global LAST_ID
    con = sqlite3.connect('databasepi')
    cur = con.cursor()
    if str(args[1]) == "OK":
        sql_statement = """UPDATE data
                            SET has_been_send=1
                            WHERE ID = {};""".format(LAST_ID)
        print(sql_statement)
        cur.execute(sql_statement)
        con.commit()
        con.close()

def on_message(client, userdata, message):
    data = json.loads(message.payload)
    print(dir(message))
    date_now = datetime.now()
    time_formatted = date_now.strftime("%A %B %d, %Y, %H:%M:%S")
    has_been_send = IOT.iot_send_message(data['client_id'], data['temperature'], data["humidity"], data["pressure"], callback=iot_callback)
    insert_into_db(data)

def loop():
    print("ayo its working boyy")
    time.sleep(60)
    

def insert_into_db(data):
    global LAST_ID
    # has to be here cuz threads idk why
    con = sqlite3.connect('databasepi')
    cur = con.cursor()

    date_now = datetime.now()
    time_formatted = date_now.strftime("%A %B %d, %Y, %H:%M:%S")
    sql_statement = "INSERT INTO data (device_id, temperature, humidity, pressure, rpi_datetime, sensor_datetime, has_been_send) VALUES ('{}', {}, {}, {}, '{}', '{}', {});".format(str(data["client_id"].strip("b").strip("'")), data["temperature"], data["humidity"], data["pressure"], time_formatted, data["timestamp"], 0)
    print(sql_statement)
    cur.execute(sql_statement)
    LAST_ID = cur.lastrowid
    con.commit()
    con.close()

def create_table():
    con = sqlite3.connect('databasepi')
    cur = con.cursor()
    sql_statement = """CREATE TABLE "data2" (
	"ID"	INTEGER NOT NULL UNIQUE,
	"device_id"	TEXT,
	"pressure"	REAL,
	"temperature"	REAL,
	"humidity"	REAL,
	"sensor_datetime"	TEXT,
	"rpi_datetime"	TEXT DEFAULT current_timestamp,
	"has_been_send"	INTEGER DEFAULT 0,
	PRIMARY KEY("ID" AUTOINCREMENT)
    );"""
    cur.execute(sql_statement)
    con.commit()
    con.close()


if __name__ == "__main__":
    client = mqtt.Client(client_id="PI")
    client.username_pw_set("luni", "12345")
    client.on_message=on_message
    client.connect("127.0.0.1")
    # con = sqlite3.connect('databasepi')
    # cur = con.cursor()
    IOT.iot_init()
    client.subscribe("data")
    # t1 = threading.Thread(target=loop())
    # t1.start()
    client.loop_forever()
    exit()