
# ______________________________________________________________________________________________________________________________

# @Author Ties Klappe
# @Date   31/03/2017
# @Desc   MQTT client thuisnetwerk

# ______________________________________________________________________________________________________________________________

''' Error messages description:

                  #C0001: Corrupted or non-numeric MS values.
                  #C0002: Incomplete or wrong MS message.
                  #C0003: Unknown Sub_type, please update function get_field_id within this module.
    '''

import paho.mqtt.client as mqtt
import re
import csv
import datetime
import time


class DataProcessor:
    # IP of the remote server where the MQTT telegraf broker runs
    LXC_IP = "broker.shiftr.io"
    # Standard MQTT port, if undefined will be 1883
    MQTT_PORT = 1883
    # Time to live before the message is killed
    TTL = 60
    # Topic which will be published to
    PBL_TOPIC = "/temp"
    # Number of MySensor parameters
    NR_OF_PARAMETERS = 6
    # Quality of Serivce, can be 0 or 1: by default 0
    QOS = 1

    # Function on_connect will print the connection status between Controller and Telegraf Broker. The following results are possible:

    # RC 0: Connection successfully established
    # RC 1: Connection refused: used protocol version unsupported
    # RC 2: Connection refused: invalid client identifier
    # RC 3: Server unavailable
    # RC 4: Bad Username or Password
    # RC 5: Unauthorised

    # declares all the subtypes
    subtypes = {'temp': '0', 'hum': '1', 'status': '2', 'light': '23', 'rain': '6', 'door': '16'}

    def on_connect(client, userdata, rc):
        print("Connection returned result: " + connack_string(rc))




    # Constructor to inialize networkid, MQTT client and default messages

    def __init__(self, networkid):

        self.networkid = networkid
        self.client = mqtt.Client("Publisher", True, None, mqtt.MQTTv31)
        self.client.on_connect = self.on_connect



    # Function data_is_valid checks to see if the received message is a MySensors 2.0 measurement.

    def data_is_valid(self, line):
        # print line
        if line.count(';') == 5:
            element = line.split(';', self.NR_OF_PARAMETERS)

            if element[0] == 0:
                print "NodeID 0 has been reserved for gateway. Error message #C0001.\n"
                return False
            
            elif element[0].isdigit() and element[1].isdigit() and element[2].isdigit() and element[3].isdigit() and \
                            element[4].isdigit() == True:
                return True
            else:
                print "Data contains corrupted values. Error message #C0001.\n"
                return False
        else:
            print "Invalid data request. Error message #C0002.\n"

            return False





    # Function get_field_id(self, returns field ID for data point based on sub_type.

    # Source for index: https://www.mysensors.org/download/serial_api_20


    def get_field_id(self, sub_type, node_id):

        if (str(sub_type) == self.subtypes['temp']):
            self.PBL_TOPIC = "Temperature/" + str(node_id)
            return "temp"

        elif (str(sub_type) == self.subtypes['hum']):
            return "hum"

        elif (str(sub_type) == self.subtypes['status']):
            return "status"

        elif (str(sub_type) == self.subtypes['light']):
            return "light"

        elif (str(sub_type) == self.subtypes['rain']):
            return "rain"

        elif (str(sub_type) == self.subtypes['door']):
            return "door"
        else:
            #print "subtype is: " + str(sub_type)
            print "Above sub_type is unknown. Error message #C0003."
            return "Error"




    # Data format for the MQTT broker to accept is Influx.

    # Function parse_data turns a line into an accepted Telegraf meatric according to the InfluxData data format.

    def parse_data(self, line, type):
        element = line.split(';', self.NR_OF_PARAMETERS)
        field_id = str(self.get_field_id(element[4], element[0]))
        measurement_value = element[5]
        measurement_value_parsed = re.sub('[^0-9.]', '', measurement_value)
        #print repr(measurement_value_parsed)

        if (field_id == "error"):
            return "Error"

        if (type == "mqtt"):
            return field_id + ",networkid=" + str(self.networkid) + ",nodeid=" + element[0] + ",sensorid=" + element[
                1] + ",messagetype=" + element[2] + ",ack=" + element[3] + ",subtype=" + element[
                       4] + " value=" + measurement_value_parsed

        else:
            #Pushes data to the CSV file.
            #Get the datetime
            datetime.datetime.time(datetime.datetime.now())
            now = datetime.datetime.now()
            #convert to ms timestamp
            t = time.mktime(now.timetuple())
            #convert the time to a string
            timeToString = str(t)

            return element[0] + "," + element[1] + "," + element[4] + "," + measurement_value_parsed + "," + timeToString + "\n"



    # Function push_remote will push parsed data with MQTT to the Broker.

    def push_remote(self, line):

        self.client.username_pw_set("0f5b221e", "bc0310f841027b4a")
        self.client.connect("broker.shiftr.io", 1883, 60)
        #self.client.publish(self.PBL_TOPIC, str(line), qos=self.QOS)
        self.client.publish(self.PBL_TOPIC, str(line), qos=1)
