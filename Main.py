from fitparse import FitFile
from math import pow
from psycopg2 import *
import glob, os

# define Garmin data to be added as database columns along with data type
keys = {
    'altitude': 'DECIMAL',
    'heart_rate': 'INTEGER',
    'position_lat': 'LATITUDE',
    'position_long': 'LONGITUDE',
    'speed': 'DECIMAL',
    'temperature': 'DECIMAL',
    'timestamp': 'TIMESTAMP'
}

# add path to older containing FIT files
os.chdir("path/to/files")

# add connection details of PostGIS db
connection = connect("dbname=*** user=*** password=***")
cursor = connection.cursor()

# name of table 
name = 'waypoints'



####################################################################

def semicircle2degree(value):
    return value * (180 / pow(2, 31))


def insert_values(name, key_dict, file):
    keys = key_dict.keys()

    for record in file.get_messages('record'):
        insert_statement = "INSERT INTO " + name + "("
        key_string = ""
        value_string = ""
        latitude = -999
        longitude = -999

        for record_data in record:

            if record_data.value is not None and record_data.name in keys and key_dict[record_data.name] == 'LATITUDE':
                latitude = semicircle2degree(record_data.value)

            elif record_data.value is not None and record_data.name in keys and key_dict[
                record_data.name] == 'LONGITUDE':
                longitude = semicircle2degree(record_data.value)

            elif record_data.name in keys and record_data.value is not None and record_data.value != 0:
                key_string += record_data.name + ", "

                if type(record_data.value) is int and record_data.units and record_data.units == 'semicircles':
                    record_data.value = semicircle2degree(record_data.value)

                if record_data.name == 'timestamp':
                    record_data.value = "'" + str(record_data.value) + "'"

                value_string += str(record_data.value) + ", "

        if longitude != -999 and latitude != -999:
            key_string += "the_geom, "
            value_string += "ST_GeomFromText('POINT(" + str(longitude) + " " + str(latitude) + ")', 4326), "

        insert_statement += key_string[:-2] + ")\n VALUES\n(" + value_string[:-2] + ");\n"
        # print insert_statement
        cursor.execute(insert_statement)
    connection.commit()


def create_table(name, keys):
    has_latitude = None
    has_longitude = None
    create_statement = "CREATE TABLE " + name + " (\n id SERIAL PRIMARY KEY"

    for key, value in keys.iteritems():
        if value == 'LONGITUDE':
            has_longitude = True
        elif value == 'LATITUDE':
            has_latitude = True
        else:
            create_statement += ",\n " + key + " " + value
    create_statement += "\n);"

    if has_latitude and has_longitude:
        create_statement += "\nSELECT AddGeometryColumn('" + name + "', 'the_geom', '4326', 'POINT', 2);"

    cursor.execute(create_statement)
    connection.commit()

create_table(name, keys)
for file in glob.glob("*.FIT"):
    fitfile = FitFile(file)
    insert_values(name, keys, fitfile)

cursor.close()
connection.close()



