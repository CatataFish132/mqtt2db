#!/usr/bin/env python#/bin/env/python
import sqlite3
import paho.mqtt.client as mqtt
import json
from datetime import datetime
import app as IOT
import threading
import time
import sched
import configparser
import pytz

LAST_ID = None
tz = pytz.timezone("Europe/Amsterdam")

def iot_callback(*args, **kwargs):
    '''this function gets called when we get a callback from the IOT hub. if it received the message it updates the row in the database that the data has been received'''

    ID = args[0].message_id
    con = sqlite3.connect(cf["DATABASE"]["path"])
    cur = con.cursor()
    if str(args[1]) == "OK":
        sql_statement = """UPDATE data
                            SET has_been_send=1
                            WHERE ID = {};""".format(ID)
        cur.execute(sql_statement)
        con.commit()
        con.close()

def on_message(client, userdata, message):
    '''this function gets called when an MQTT message arrives and sends a message to the IOT hub with the data is receives'''

    data = json.loads(message.payload)
    date_now = datetime.now(tz=tz)
    time_formatted = date_now.strftime("%A %B %d, %Y, %H:%M:%S")
    ID = insert_into_db(data)
    IOT.iot_send_message(data['client_id'], data['temperature'], data["humidity"], data["pressure"], date_created=time_formatted, callback=iot_callback, ID=ID)
    # delete the latest rows if the database hold more than 200 rows
    check_latest_value()

def resend(sc):
    '''this function runs in a loop and resends messages that havent been received by the IOT hub'''

    # send previously unsend datapoints
    con = sqlite3.connect(cf["DATABASE"]["path"])
    cur = con.cursor()
    sql_statement = """SELECT ID, device_id, temperature, humidity, pressure, rpi_datetime FROM data WHERE has_been_send=0;"""
    has_not_been_send = cur.execute(sql_statement)
    data = has_not_been_send.fetchall()

    # loop through all unsend data and resends them
    for measurement in data:
        print(measurement)
        ID, device_id, temperature, humidity, pressure, rpi_datetime = measurement
        date_time = datetime.strptime(rpi_datetime, "%Y-%m-%d %H:%M:%S.%f")
        rpi_datetime = date_time.strftime("%A %B %d, %Y, %H:%M:%S")
        IOT.iot_send_message(device_id=device_id, temperature=temperature, humidity=humidity, pressure=pressure, date_created=rpi_datetime, callback=iot_callback, ID=ID)
    sc.enter(60, 1, resend, (sc,))

def insert_into_db(data):
    '''Inserts the data in the database'''

    # con = sqlite3.connect('databasepi')
    con = sqlite3.connect(cf["DATABASE"]["path"])
    cur = con.cursor()

    # get datetime
    date_now = datetime.now(tz=tz)
    # time_formatted = date_now.strftime("%A %B %d, %Y, %H:%M:%S")

    # formats client_id in a format we can send to the database
    data["client_id"] = str(data["client_id"]).strip("b").strip("'")
    sql_statement = "INSERT INTO data (device_id, temperature, humidity, pressure, rpi_datetime, sensor_datetime, has_been_send) VALUES ('{}', {}, {}, {}, '{}', '{}', {});".format(data["client_id"], data["temperature"], data["humidity"], data["pressure"], str(date_now).replace("+02:00", ''), data["timestamp"], 0)
    
    # execute sql statement and save database
    cur.execute(sql_statement)
    ID = cur.lastrowid
    con.commit()
    con.close()
    return ID

def check_latest_value():
    '''delete the latest rows if the database holds more than 200 received rows'''

    sql_statement = "SELECT COUNT(ID) FROM data WHERE has_been_send=1;"
    con = sqlite3.connect(cf["DATABASE"]["path"])
    cur = con.cursor()
    count = cur.execute(sql_statement).fetchone()
    count = count[0]
    if count > 200:
        amount = count - 200
        # sql_statement = "DELETE FROM data ORDER BY ID ASC LIMIT %d;" % amount
        sql_statement = "DELETE FROM data WHERE has_been_send=1 ORDER BY ID ASC LIMIT %d;" % amount
        cur.execute(sql_statement)
        con.commit()
    con.close()

def create_table():
    '''Creates the table in the database'''

    con = sqlite3.connect(cf["DATABASE"]["path"])
    cur = con.cursor()
    sql_statement = """CREATE TABLE "data" (
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
    # read config
    cf = configparser.ConfigParser()
    cf.read("config.ini")

    # connect to mqtt broker
    client = mqtt.Client(client_id=str(cf["MQTT"]["client_id"]))
    client.username_pw_set(str(cf["MQTT"]["username"]), str(cf["MQTT"]["password"]))
    client.on_message=on_message
    client.connect("127.0.0.1")
    IOT.iot_init()
    client.subscribe("data")

    # make an eventloop call the resend function every 60 seconds
    s = sched.scheduler(time.time, time.sleep)
    s.enter(5, 1, resend, (s,))
    t1 = threading.Thread(target=s.run)
    t1.daemon = True
    t1.start()
    client.loop_forever()
    