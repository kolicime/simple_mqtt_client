import paho.mqtt.client as mqtt
import pymysql
import json


def on_connect(client, userdata, flags, rc):
    print("Connected with result code " + str(rc))
    # 在规则引擎设置了转发后方可收到信息
    client.subscribe("/sys/yourProductKey/yourDeviceName/thing/service/property/set")


def on_message(client, userdata, msg):
    payload = msg.payload.decode('utf-8')
    payload = json.loads(payload)
    print(payload)
    product_id = payload['productKey']
    device_id = payload['deviceName']
    a = payload['items']
    humi = a['CurrentHumidity']
    hum = humi['value']
    # print(humi['time'])
    # 把时间矫正到北京时间，时间戳为毫秒级
    hum_time = humi['time'] + 8 * 60 * 60 * 1000
    print(type(hum_time))
    # hum_time = hum_time.datatime.datetime.strftime("%Y-%m-%d %H:%M:%S")
    hum_time = str(hum_time)
    print(type(hum_time))
    temp = a['CuTemperature']
    tem = temp['value']
    tem_time = temp['time']
    db = pymysql.connect("127.0.0.1", "root", "admin", "fengxiang")
    cursor = db.cursor()
    sql = "insert into state(`tem`,`hum`,`time`, `product_id`, `device_id`) values ('%d','%d','%s', '%s', '%s')" % (tem, hum, hum_time, product_id, device_id)
    try:
        cursor.execute(sql)
        db.commit()
        print("导入完成")
    except:
        db.rollback()
        print("导入失败")
    db.close()


# 以下信息可在iot平台获取，其中DeviceName与上面的不一样
host = "yourProductKey.iot-as-mqtt.cn-shanghai.aliyuncs.com"
client = mqtt.Client("yourDeviceName|securemode=3,signmethod=hmacsha1,timestamp=789|")
client.username_pw_set("yourDeviceName&yourProductName", "由工具计算得来的passwd")
client.on_connect = on_connect
client.on_message = on_message
client.connect(host, 1883, 60)
client.loop_forever()