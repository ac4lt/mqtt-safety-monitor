# ASCOM Alpaca Observing Conditions low level device
#
# This class is meant to be used from the high level ObservingConditions class
#
# linda 2023-12-25 initial coding

import paho.mqtt.client as mqtt
import time
from logging import Logger
from threading import Lock
from config import Config

class MQTTSafetyMonitor:

    # lock
    _lock = None

    # observing conditions values
    _cloudCover = 100
    _probabilityOfPrecipitation = 100
    _forecastPrecipitation = 999
    _rainInRegion = True
    _rainRate = 1
    _limit_precipation_probability = 5
    _windSpeedAvg = 100

    # to calculate rain rate
    last_rain_reading = 0
    current_rain_reading = 0
    last_rain_time = 0
    current_rain_time = 0


    def __init__(self, logger):
        # internal state
        self.logger = logger
        self._connected = False
        self._lock = Lock()
        # MQTT client
        self.client = mqtt.Client("MQTTSafetyMonitor", userdata = self)
        self.client.username_pw_set(Config.mqtt_user, Config.mqtt_password)
        self.client.connect(Config.mqtt_server, Config.mqtt_port)
        # safety limit for probability of precipitation
        _limit_precipation_probability = Config.limit_precipation_probability
        # add callbacks
        self.client.on_connect = on_connect
        self.client.on_disconnect = on_disconnect
        self.client.message_callback_add(Config.topic_cloud_cover, on_message_cloud_cover)  
        self.client.message_callback_add(Config.topic_probability_of_precipitation, on_message_probability_of_precipitation)  
        self.client.message_callback_add(Config.topic_event_rain, on_message_event_rain)        
        self.client.message_callback_add(Config.topic_forecast_precipitation, on_message_forecast_precipitation_mm)     
        self.client.message_callback_add(Config.topic_rain_in_region, on_message_rain_in_region)   
        self.client.message_callback_add(Config.topic_wind_speed_avg, on_message_wind_speed_avg)
        # subscriptions
        self.client.subscribe(Config.topic_cloud_cover)
        self.client.subscribe(Config.topic_probability_of_precipitation)     
        self.client.subscribe(Config.topic_event_rain)   
        self.client.subscribe(Config.topic_forecast_precipitation)
        self.client.subscribe(Config.topic_rain_in_region)
        self.client.subscribe(Config.topic_wind_speed_avg)
        # publish that we are alive
        self.client.publish("client/mqtt-safety-monitor", True, retain=True)
        self.client.will_set("client/mqtt-safety-monitor", False, retain=True)
        # start handling subscriptions
        self.client.loop_start()

    @property
    def connected(self) -> bool:
        self._lock.acquire()
        res = self._connected
        self._lock.release()
        return res
    @connected.setter
    def connected (self, connected: bool):
        self._lock.acquire()
        self._connected = connected
        self._lock.release()
        if connected:
            self.logger.info('[connected]')
        else:
            self.logger.info('[disconnected]')    


    @property
    def issafe(self) -> bool:
        self._lock.acquire()
        res =  self._cloudCover != 100 and \
            self._probabilityOfPrecipitation <= self._limit_precipation_probability and \
            self._rainRate == 0 and \
            self._forecastPrecipitation == 0 and \
            not self._rainInRegion and self._windSpeedAvg < 10
        self.logger.info(f"cloud cover: {self._cloudCover}, pop: {self._probabilityOfPrecipitation}, " +
                         "rain rate: {self._rainRate}, forecast precip: {self._forecastPrecipitation}, " +
                         "rain in region: {self._rainInRegion}")
        self._lock.release()
        return res     
    
# callbacks    
def on_connect(client, userdata, flags, rc):
    userdata.logger.info(f"[mqtt] connected rc = {rc}, flags={flags}")

def on_disconnect(client, userdata, rc):
    userdata.logger.info(f"[mqtt] disconnected, rc={rc}")

def on_message_cloud_cover(client, userdata, msg):
    userdata._lock.acquire()
    userdata._cloudCover = float(msg.payload.decode('utf-8'))
    userdata.logger.info(f"[cloud cover] value {userdata._cloudCover}")  
    userdata._lock.release()

def on_message_probability_of_precipitation(client, userdata, msg):
    userdata._lock.acquire()
    userdata._probabilityOfPrecipitation = float(msg.payload.decode('utf-8')) 
    userdata.logger.info(f"[probability of precipitation] value {userdata._probabilityOfPrecipitation}")  
    userdata._lock.release()

def on_message_event_rain(client, userdata, msg):
    userdata._lock.acquire()
    userdata.last_rain_reading = userdata.current_rain_reading
    userdata.current_rain_reading = float(msg.payload.decode('utf-8'))
    userdata.last_rain_time = userdata.current_rain_time
    userdata.current_rain_time = time.time()
    rate = (userdata.current_rain_reading - userdata.last_rain_reading) / (userdata.current_rain_time - userdata.last_rain_time)
    if rate < 0:
        rate = 0
    userdata._rainRate = rate * 3600  # convert mm/s to mm/hour
    userdata.logger.info(f"[rain rate] value {userdata._rainRate}")  
    userdata._lock.release()

def on_message_forecast_precipitation_mm(client, userdata, msg):
    userdata._lock.acquire()
    userdata._forecastPrecipitation = float(msg.payload.decode('utf-8'))
    userdata.logger.info(f"[forecast precipitation] value {userdata._forecastPrecipitation}")  
    userdata._lock.release()

def on_message_rain_in_region(client, userdata, msg):
    userdata._lock.acquire()
    userdata._rainInRegion = str(msg.payload.decode('utf-8')) == "true" 
    userdata.logger.info(f"[rain in region] value {userdata._rainInRegion}")  
    userdata._lock.release()

def on_message_wind_speed_avg(client, userdata, msg):
    userdata._lock.acquire()
    userdata._windSpeedAvg = float(msg.payload.decode('utf-8')) * (1000.0/3600.0)  # convert from km/h to m/s
    userdata.logger.info(f"[avg wind speed] value {userdata._windSpeedAvg}")  
    userdata._lock.release()    
