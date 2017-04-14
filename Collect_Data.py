
# ______________________________________________________________________________________________________________________________

# @Author Ties Klappe
# @Date   31/03/2017
# @Desc   MQTT client thuisnetwerk

# ______________________________________________________________________________________________________________________________

import serial
import Process_Data


class DataCollector:


    # Constructor to inialize the serial port, baudrate and networkid

    def __init__(self, serialport, baudrate, networkid):

        self.baudrate = baudrate
        self.serialport = serialport
        self.networkid = networkid
        
        self.ser = ''




    # Function run_collector will open up the serial port, collect incoming messages, validate
    # those messages and store them while using functions from the DataProcessor class.

    def run_collector(self):

        self.open_port()
        PD = Process_Data.DataProcessor(self.networkid)
        while 1:
            incoming_message = str(self.ser.readline())

            if "Gateway" in incoming_message:

                # print "Gateway inialization started."
                pass



            elif (incoming_message and PD.data_is_valid(incoming_message) == True):
                parsed_mqtt_message = PD.parse_data(incoming_message, "mqtt")

                if not ("Error" in parsed_mqtt_message):
                    PD.push_remote(parsed_mqtt_message)

                else:
                    pass



    # Function open_port will open up the selected serial port which was given at starting up the MQTT controller.

    def open_port(self):

        self.ser = serial.Serial(
            port = self.serialport,
            baudrate = self.baudrate,
            timeout = 1
    )
