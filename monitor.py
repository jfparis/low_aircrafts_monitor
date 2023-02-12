import json
import os.path
import time
from datetime import datetime

import cachetools
import geopy.distance
import paho.mqtt.client as mqtt
import requests

import config

ttl_cache = cachetools.TTLCache(maxsize=128, ttl=10 * 60)
count = 0
earliest_aircraft = None
lowest_aircraft = None

last_run = None


def poll(client, topic):
    global last_run, count, earliest_aircraft, lowest_aircraft
    now = datetime.utcnow()
    if last_run is not None and last_run.hour < 3 and now.hour >= 3:
        count = 0
        lowest_aircraft = None
        earliest_aircraft = None

    last_run = now
    r = requests.get(config.FEEDER_URL)
    data = r.json()
    for each in data["aircraft"]:

        if set(each.keys()).issuperset(set(["lat", "lon", "alt_baro", "flight"])):

            try:
                plane_cord = (each["lat"], each["lon"])  # TODO: add error handling
                dist = round(geopy.distance.geodesic(config.HOME, plane_cord).m, 0)
                altitude = round(
                    float(each["alt_baro"]) * 0.3048, 0
                )  # add error handling
            except Exception as err:
                print(err)
                next

            if altitude <= config.THRESHOLD_ALT and dist < config.THRESHOLD_DIST:

                if each["flight"] not in ttl_cache.keys():

                    stamp = now.strftime("[%Y-%m-%d %H:%M:%S]")
                    print(
                        f"{stamp} LOW PASS : {each['hex']} - {each['flight']} - distance: {dist} - altitude: {altitude}"
                    )
                    count = count + 1
                    print(f"{stamp} So far today we have had {count} low passes")

                    ttl_cache[each["flight"]] = True

                    if earliest_aircraft is None:
                        earliest_aircraft = now

    payload = {}
    payload["count"] = count
    payload["earliest_aircraft"] = (
        earliest_aircraft.strftime("%H:%M") if earliest_aircraft is not None else None
    )
    # payload["friendly_name"] = "Daily counter of low airplane passes"
    state_topic = os.path.join(topic, "state")
    client.publish(state_topic, json.dumps(payload))

    low_aircraft_count_conf = {
        "object_id": config.UNIQUE_ID,
        "state_topic": state_topic,
        "json_attributes_topic": state_topic,
        "state_class": "total_increasing",
        "icon": "mdi:airplane-landing",
        "name": "Daily counter of low airplane passes",
        "value_template": "{{ value_json.count}}",
        "unique_id": config.UNIQUE_ID,
    }
    config_topic = os.path.join(topic, "config")
    client.publish(config_topic, json.dumps(low_aircraft_count_conf))


def main():
    client = mqtt.Client(config.UNIQUE_ID)
    client.username_pw_set(config.BROKER_USER, password=config.BROKER_PASSWORD)
    client.connect(config.BROKER_ADDRES)
    client.loop_start()

    topic_state = config.MQTT_ROOT

    while True:
        poll(client, topic_state)
        time.sleep(5)


if __name__ == "__main__":
    main()
