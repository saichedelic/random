import time

import datetime
import utilities
import ordereddict
import dateutil.tz
import mongoInterface
import influxInterface
import loggerInterface

logger = loggerInterface.LoggerManager().getLogger('dataWriter')

class settings:
    def __init__(self):
        pass

    def changePassword(self, id, username, password):
        mongo_users = mongoInterface.dbBase('users')
        status, userData = mongo_users.findOne({'username': username})
        if not status or not userData or userData.get('username', '') != username:
            return False, 'Requested user not available!....'
        return mongo_users.modifyData(userData['_id'], {'password': password})

    def deleteSensor(self, id, sensorId):
        return True, "Success"
        status, data = sensor_list().get({'sensorId': sensorId})
        if status and data and data.get('data', []):
            query = "DROP SERIES FROM  waterMeter where sensorId = '%s'" % sensorId
            intf = influxInterface.InfluxInterface()
            intf.getConnection()
            intf.connection.query(query)
            logger.info("Deleteing Data For Sensor ID: %s" % sensorId)
            sensor_list().deleteData({'sensorId': sensorId})
            return True, "Success"
        return False, "Sensor Not Available"


class zones(mongoInterface.dbBase):
    def __init__(self):
        super(zones, self).__init__('zones')

    def updateZones(self, document):
        # return True, 'Success'
        names = []
        logger.info('ZoneData: %s' % document)
        for zone in document:
            if not zone.get('name'):
                continue
            if zone.get('name', '') in names:
                return False, 'Same name repeated'
            names.append(zone.get('name', ''))

        for zone in document:
            if zone.get('_id', None):
                self.modifyData(zone.get('_id'), zone)
            else:
                if not zone.get('name', ''):
                    continue
                self.createData(zone)
            # names.append(zone.get('name'))
        status, zoneData = self.get({'name': {'$nin': names}}, limit=0, projection=['name'])
        if status and zoneData and zoneData.get('data', []):
            for zone in zoneData.get('data', []):
                status, sensorList = sensor_list().get({'zoneId': str(zone.get('_id', ''))})
                if status and sensorList and sensorList.get('data', []):
                    for sensor in sensorList['data']:
                        sensor_list().modifyData(sensor['_id'], {'zoneId': "0"})
                super(zones, self).deleteData(zone.get('_id', ''))
        return True, 'Success'


class sensor_data(mongoInterface.dbBase):
    def __init__(self):
        super(sensor_data, self).__init__('sensor_data')


class sensor_list(mongoInterface.dbBase):
    def __init__(self):
        super(sensor_list, self).__init__('sensor_list')


class graphs(object):

    def _convertTimeZone(self, timeData):
        newZone = []
        for timeVal in timeData:
            utc = datetime.datetime.strptime(timeVal,'%Y-%m-%dT%H:%M:%SZ')
            from_zone = dateutil.tz.tzutc()
            to_zone = dateutil.tz.tzlocal()
            utc = utc.replace(tzinfo=from_zone)
            central = utc.astimezone(to_zone)
            newZone.append(central.strftime("%Y-%m-%dT%H:%M:%SZ"))
        return newZone

    def getZoneWiseData(self, id=None, period="today", sensorId=""):
        status, data = zones().get(limit=0)
        zoneData = {str(zone.get('_id', '')): zone.get('name') for zone in data.get('data', [])}
        status, sensorList = sensor_list().get(limit=0)
        sensorMap = {zoneId: [] for zoneId in zoneData}
        for sensor in sensorList.get('data', []):
            if sensor.get('zoneId'):
                sensorMap[sensor.get('zoneId')].append(sensor.get('sensorId'))
        returnData = []
        toTime = int(round(time.time() * 1000))
        fromTime = toTime - int(round(utilities.getPeriodData(period) * 1000))
        interval = utilities.getPeriodInterval(period)
        intf = influxInterface.InfluxInterface()
        for zone, sensors in sensorMap.items():
            print(sensors)
            # query = """SELECT non_negative_derivative(sum("value"), %s) FROM waterMeter WHERE sensorId =~ /^%s$/ and time >= %sms AND time <= %sms group by time(%s) fill(null)""" % (interval, "|".join(sensors), fromTime, toTime, interval)
            query = """SELECT sum("value") FROM waterMeter WHERE sensorId =~ /^(%s)$/ and time >= %sms AND time <= %sms group by time(%s) fill(null)""" % (
            "|.*".join(sensors), fromTime, toTime, interval)
            logger.info(query)
            status, sensorData = intf.getGraphData(query, False, True)
            if status:
                if len(sensorData):
                    sensorData[0]["y"] = [y if y else 0 for y in sensorData[0]["y"]]
                    sensorData[0]["x"] = self._convertTimeZone(sensorData[0]["x"])
                    sensorData[0]['name'] = zoneData.get(zone, "") + "(%s)" % sum([y if y else 0 for y in sensorData[0]["y"]])
                    # sensorData[0]["type"] = "bar"
                else:
                    sensorData = [{"name": zoneData.get(zone, ""), "x": [], "y": []}]
                returnData.extend(sensorData)
        status, data = zones().get(limit=0)
        zoneData = {str(zone.get('_id', '')): zone.get('name') for zone in data.get('data', [])}
        status, sensorList = sensor_list().get(limit=0)
        sensorMap = {}
        for sensor in sensorList.get('data', []):
            if sensor.get('zoneId'):
                sensorMap[sensor.get('sensorId')] = zoneData.get(sensor.get('zoneId'))
        query = """SELECT sum(value) FROM waterMeter WHERE  time >= %sms AND time <= %sms GROUP BY sensorId, sensorId fill(null)""" % (
        fromTime, toTime)
        logger.info(query)
        intf = influxInterface.InfluxInterface()
        status, sensorData = intf.getGraphData(query, False)
        data = []
        if status:
            data = [{'sensorId': value.get('tags', {}).get('sensorId', ''), "value": value.get('values')[0][1]} for
                    value in sensorData if
                    value.get('values') and len(value.get('values')[0]) > 1 and value.get('tags', {}).get('sensorId',                                                                                                          '')]
        zoneWiseData = {}
        for sensor in data:
            if sensorMap.get(sensor.get('sensorId')) not in zoneWiseData:
                zoneWiseData[sensorMap.get(sensor.get('sensorId'))] = []
            zoneWiseData[sensorMap.get(sensor.get('sensorId'))].append(sensor)
        return True, {'graph': returnData, 'table': zoneWiseData}

    def getGraph(self, id=None, period="today", sensorId="All"):
        status, sensorList = sensor_list().get(limit=0)
        sensorMap = {}
        for sensor in sensorList.get('data', []):
            if sensor.get('zoneId'):
                sensorMap[sensor.get('sensorId')] = sensor.get('description', sensor.get('sensorId'))
        toTime = int(round(time.time() * 1000))
        fromTime = toTime - int(round(utilities.getPeriodData(period) * 1000))
        intf = influxInterface.InfluxInterface()
        if sensorId == "All":
            status, intfData = intf.getGraphData('show series From waterMeter')
        else:
            intfData = [{'y': [sensorId]}]
        returnData = []
        interval = utilities.getPeriodInterval(period)
        for sensorId in intfData[0]['y']:
            query = """SELECT mean("value") FROM "waterMeter" WHERE ("sensorId" = '%s') AND time >= %sms AND time <= %sms GROUP BY time(%s) fill(null)""" % (sensorId, fromTime, toTime, interval)
            logger.info(query)
            status, sensorData = intf.getGraphData(query)
            if status:
                if len(sensorData):
                    sensorData[0]['name'] = sensorMap.get(sensorId)
                    sensorData[0]["x"] = self._convertTimeZone(sensorData[0]["x"])
                    sensorData[0]["y"] = [y if y else 0 for y in sensorData[0]["y"]]
                else:
                    sensorData = [{"name": sensorId, "x": [], "y": []}]
                returnData.extend(sensorData)
        return True, returnData

    # For Getting Total Usage
    def getTableData(self, id=None, period="today"):
        toTime = int(round(time.time() * 1000))
        fromTime = toTime - int(round(utilities.getPeriodData(period) * 1000))
        query = """SELECT sum(value) FROM waterMeter WHERE  time >= %sms AND time <= %sms GROUP BY sensorId, sensorId fill(null)""" % (fromTime, toTime)
        logger.info(query)
        intf = influxInterface.InfluxInterface()
        status, sensorData = intf.getGraphData(query, False)
        data = []
        if status:
            data = [{'sensorId': value.get('tags', {}).get('sensorId', ''), "value": value.get('values')[0][1]} for value in sensorData if value.get('values') and len(value.get('values')[0]) > 1 and value.get('tags', {}).get('sensorId', '')]
        return True, data

    def getZonewiseTableData(self, id=None, period="today"):
        status, data = zones().get(limit=0)
        zoneData = {str(zone.get('_id', '')): zone.get('name') for zone in data.get('data', [])}
        status, sensorList = sensor_list().get(limit=0)
        sensorMap = {}
        for sensor in sensorList.get('data', []):
            if sensor.get('zoneId'):
                sensorMap[sensor.get('sensorId')] = zoneData.get(sensor.get('zoneId'))
        status, data = self.getTableData('', period)
        zoneWiseData = {}
        for sensor in data:
            if sensorMap.get(sensor.get('sensorId')) not in zoneWiseData:
                zoneWiseData[sensorMap.get(sensor.get('sensorId'))] = []
            zoneWiseData[sensorMap.get(sensor.get('sensorId'))].append(sensor)
        return True, zoneWiseData