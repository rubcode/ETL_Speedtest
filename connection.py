from dotenv import load_dotenv
import mysql.connector
import os

def connect():
    dbCon = mysql.connector.connect(
        host= os.getenv("host77"),
        user= os.getenv("user77"),
        password= os.getenv("pass77"),
        database= os.getenv("db77")
    )
    return dbCon

def connect81():
    dbCon = mysql.connector.connect(
        host= os.getenv("host81"),
        port= os.getenv("port81"),
        user= os.getenv("user81"),
        password= os.getenv("pass81"),
        database= os.getenv("db81")
    )
    return dbCon

def connectCloud(db):
    dbCon = mysql.connector.connect(
        host= os.getenv("hostCloud"),
        port= os.getenv("portCloud"),
        user= os.getenv("userCloud"),
        password= os.getenv("passCloud"),
        database=db
    )
    return dbCon

load_dotenv()