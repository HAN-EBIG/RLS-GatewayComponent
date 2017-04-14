
# ______________________________________________________________________________________________________________________________

# @Author Ties Klappe
# @Date   31/03/2017
# @Desc   MQTT client thuisnetwerk

# ______________________________________________________________________________________________________________________________


from Collect_Data import DataCollector
from sys import argv

script = argv

serialport = "/dev/ttyUSB0"
baudrate = 115200
networkid = 1

data_collector = DataCollector(serialport, baudrate, networkid)
data_collector.run_collector()
