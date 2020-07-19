import paho.mqtt.client as mqtt
import pymysql
import json


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    client.subscribe("/sys/a1eCBzB0UgP/1234/thing/service/property/set")


def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    payload = json.loads(payload)
    a = payload['items']
    humi = a['CurrentHumidity']
    hum = humi['value']
    # print(humi['time'])
    hum_time = humi['time']
    print(type(hum_time))
    # hum_time = hum_time.datatime.datetime.strftime("%Y-%m-%d %H:%M:%S")
    hum_time = str(hum_time)
    print(type(hum_time))
    temp = a['CuTemperature']
    tem = temp['value']
    tem_time = temp['time']
    db = pymysql.connect("127.0.0.1", "root", "admin", "fengxiang")
    cursor = db.cursor()
    sql = "insert into state(`tem`,`hum`,`time`) values ('%d','%d','%s')" % (tem, hum, hum_time)
    try:
        cursor.execute(sql)
        db.commit()
        print("导入完成")
    except:
        db.rollback()
        print("导入失败")
    db.close()


host = "a1eCBzB0UgP.iot-as-mqtt.cn-shanghai.aliyuncs.com"
client = mqtt.Client("kolicime|securemode=3,signmethod=hmacsha1,timestamp=789|")
client.username_pw_set("1234&a1eCBzB0UgP", "d940b472285623b67f4530fb423ef05bdc4a7213")
client.on_connect = on_connect
client.on_message = on_message
client.connect(host, 1883, 60)
client.loop_forever()