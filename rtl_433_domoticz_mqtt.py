import subprocess
import json
import paho.mqtt.client as mqtt
import config

def startsubprocess(command):
    proc = subprocess.Popen(command, stdout=subprocess.PIPE)
    
    while True:
        line = proc.stdout.readline()
        if line != '':
            try:
                wd = json.loads(line.rstrip())
                if wd['id'] == config.ws_id:

                    data = str(wd['temperature_C'])
                    data = data + ';' + str(wd['humidity'])
                    hum = wd['humidity']
                    if 50 <= hum <= 60:
                        hum_stat = 1
                    elif hum <= 30:
                        hum_stat = 2
                    elif hum >= 70:
                        hum_stat = 3
                    else:
                        hum_stat = 0
                    data = data + ';' + str(hum_stat)
                    th = {'idx': config.idx['th'], 'nvalue': 0, 'svalue': data}
                   
                    data = wd['direction_deg']
                    data = data + ';' + wd['direction_str']
                    data = data + ';' + str(wd['speed'] * 10)
                    data = data + ';' + str(wd['gust'] * 10)
                    data = data + ';' + str(wd['temperature_C'])
                    v = wd['speed'] * 3.6
                    t = wd['temperature_C']
                    twc = 13.12 + 0.6215 * t - (11.37 * pow (v, 0.16)) + (0.3965 * t * pow (v, 0.16));
                    data = data + ';%.1f' % twc
                    wind = {'idx': config.idx['wind'], 'nvalue': 0, 'svalue': data}
                    
                    mqttc.publish(config.mqtt_topic, json.dumps(wind))
                    mqttc.publish(config.mqtt_topic, json.dumps(th))
            except:
                print 'Not valid JSON', line.rstrip()
        else:
            break
    proc.stdout.close()

if __name__ == '__main__':
    mqttc = mqtt.Client()
    if config.mqtt_username:
        mqttc.username_pw_set(config.mqtt_username, config.mqtt_password)
    mqttc.connect(config.mqtt_host, port=config.mqtt_port)
    mqttc.loop_start()
    startsubprocess([config.rtl_433_path, '-R', '32', '-f', config.frequency, '-F', 'json'])
    mqttc.loop_stop()
    mqttc.disconnect()
    print("Closing down")
