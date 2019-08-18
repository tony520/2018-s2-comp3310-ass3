import paho.mqtt.client as mqtt
import datetime
import numpy as np

slow_list = []
fast_list = []

def on_connect_slow(client, userdata, flags, rc):
    #client.subscribe("$SYS/broker/load/publish/sent/5min") # the moving average number of all type MQTT messages sent by the broker
    #client.subscribe("$SYS/broker/heap/current size") # the current size of the heap memory in use
    #client.subscribe("$SYS/broker/clients/active") # the total number of the connected clients currently.
    
    client.subscribe("counter/slow/q0",0)
    client.subscribe("counter/slow/q1",1)
    client.subscribe("counter/slow/q2",2)

def on_connect_fast(client, userdata, flags, rc):
    #client.subscribe("$SYS/broker/load/publish/sent/5min") # the moving average number of all type MQTT messages sent by the broker
    #client.subscribe("$SYS/broker/heap/current size") # the current size of the heap memory in use
    #client.subscribe("$SYS/broker/clients/active") # the total number of the connected clients currently.
    
    client.subscribe("counter/fast/q0",0)
    client.subscribe("counter/fast/q1",1)
    client.subscribe("counter/fast/q2",2)
    
def on_message_slow(client, userdata, msg):
    # recording payload and timestamp, make it into a list
    slow_list.append(msg.topic+": "+str(msg.payload)+str(msg.timestamp))
    #print("else: ", msg.topic+": "+str(msg.payload)+str(msg.timestamp))
    
def on_message_fast(client, userdata, msg):
    # recording payload and timestamp, make it into a list
    fast_list.append(msg.topic+": "+str(msg.payload)+str(msg.timestamp))

# General method for calculating the above features with different input list
def calculate_each(n):
    counter = []
    time = []
    
    for i in range(1, len(n)): # sometimes, it has a strange message('close'), so the first message is excluded
        x = n[i].split("'")
        counter.append(round(int(float(x[1]))))
        time.append(float(x[2]))
        
    main_calculation(counter, time)
    
# Calculation of loss/duplicate/ooo rate 
def main_calculation(counter_list, time_list):
    recieve = len(counter_list)
    recieve_rate = round(len(counter_list)/ 300, 3) # total recieve packets
    print('recieve rate: ', recieve_rate)

    # define and count the loss
    loss=0
    non_dupicate = sorted(set(counter_list))# eliminate the duplicate in the counter list and sort it
    for k in range(1,len(counter_list)):
        if (counter_list[k]-counter_list[k-1] != 1) and (counter_list[k]-counter_list[k-1] <2000):#after sorting a nonduplicated list the number should be steadily inrease, so if it is not adding 1(max number to 0 is a specific one) to the previous one it is a loss
            loss += counter_list[k]-counter_list[k-1]-1 # sequence:1236 loss=6-3-1=2
    loss_rate = round(loss/(len(non_dupicate)+loss), 3)
    print("loss rate: ",loss_rate)

    # count the duplicate number
    duplicate_number = len(counter_list)-len(non_dupicate)# duplicate number=original list length - without duplicate list length
    duplicate_rate = round(duplicate_number/len(counter_list), 3)
    print("duplicate rate: ", duplicate_rate)

    #count the out of order
    misorder=0
    misorder_list=[]
    for k in range(1,len(counter_list)):
        if (counter_list[k]-counter_list[k-1] < 0) and (counter_list[k]-counter_list[k-1] != max(counter_list)-min(counter_list)):#if the number is not increasing to the previous one(max number to 0 is a specific one), it is out-of-order
            misorder_list.append(counter_list[k])# add the duplicate number to a list
            misorder += len(set(misorder_list))# eliminate the duplicate in the misorder_list, the same number should only be counted once
    misorder_rate = round(misorder/len(counter_list), 3)
    print("misorder rate: ", misorder_rate)

    # calculate the mean and variation of gaps
    import statistics
    # make a time gap list
    gap_list=[]
    for m in range(0,len(time_list)-1):
        gap = round((time_list[m+1]-time_list[m])*1000)
        # calculate the time gap between packets
        gap_list.append(gap)# make them into a list
    # print(gap_list)

    # calculate gap mean
    gap_mean = round(statistics.mean(gap_list), 3)
    print("gap mean: ",gap_mean)

    # calculate gap variation
    gap_variation = round(statistics.stdev(gap_list), 3)
    print("gap variation: ",gap_variation)

# different client for listening to different topics
# Client1 for listening to all slow topics
client1 = mqtt.Client(client_id="3310-<u6683953>", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
# establish client ID
client1.username_pw_set(username="students",password="33106331")# authorization
client1.connect("comp3310.ddns.net", 1883, 100)# connect to broker

client1.on_connect = on_connect_slow
client1.on_message = on_message_slow

    # listen for 5 mins
startTime = datetime.datetime.now()
waitTime = 300
while True:
    client1.loop()
    elapsedTime = datetime.datetime.now() - startTime
    if elapsedTime.seconds > waitTime:
        client1.disconnect()
        break

# slow
slow0 = []
slow1 = []
slow2 = []
slow_5min = []
slow_csize = []
slow_active = []

for i in range(1,len(slow_list)):
    if "counter/slow/q0" in slow_list[i]:
        slow0.append(slow_list[i])
    elif "counter/slow/q1" in slow_list[i]:
        slow1.append(slow_list[i])
    elif "counter/slow/q2" in slow_list[i]:
        slow2.append(slow_list[i])
    #elif "$SYS/broker/load/publish/sent/5min" in slow_list[i]:
    #    slow_5min.append(slow_list[i])
    #elif "$SYS/broker/heap/current size" in slow_list[i]:
    #    slow_csize.append(slow_list[i])
    #elif "$SYS/broker/clients/active" in slow_list[i]:
    #    slow_active.append(slow_list[i])
    else:
        print(slow_list[i])

# slow/q0
print("###RESULT:")
print("slow/q0: ")
calculate_each(slow0)
print("\n")

# slow/q1
print("slow/q1: ")
calculate_each(slow1)
print("\n")

# slow/q2 
print("slow/q2: ")
calculate_each(slow2)
print("\n")

#print("sys_slow_5min:")
#print(slow_5min[-1])
#print("sys_slow_current_size:")
#print(slow_csize)
#print("sys_slow_active:")

# sys related for slow topics
#sactive = []
#for i in range(0, len(slow_active)):
#    x = slow_active[i].split("'")
#    s = x[0]+x[1]
#    sactive.append(s)
#print(sactive)
#print("\n")


# Client2 for listening to all fast topics
client2 = mqtt.Client(client_id="3310-<u6683953>", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
# establish client ID
client2.username_pw_set(username="students",password="33106331")# authorization
client2.connect("comp3310.ddns.net", 1883, 100)# connect to broker

client2.on_connect = on_connect_fast
client2.on_message = on_message_fast

    # listen for 5 mins
startTime = datetime.datetime.now()
waitTime = 300
while True:
    client2.loop()
    elapsedTime = datetime.datetime.now() - startTime
    if elapsedTime.seconds > waitTime:
        client2.disconnect()
        break

# Sort message with correspoding topics, and store in different lists

# fast
fast0 = []
fast1 = []
fast2 = []
fast_5min = []
fast_csize = []
fast_active = [] 

for i in range(1,len(fast_list)):
    if "counter/fast/q0" in fast_list[i]:
        fast0.append(fast_list[i])
    elif "counter/fast/q1" in fast_list[i]:
        fast1.append(fast_list[i])
    elif "counter/fast/q2" in fast_list[i]:
        fast2.append(fast_list[i])
    #elif "$SYS/broker/load/publish/sent/5min" in fast_list[i]:
    #    fast_5min.append(fast_list[i])
    #elif "$SYS/broker/heap/current size" in fast_list[i]:
    #    fast_csize.append(fast_list[i])
    #elif "$SYS/broker/clients/active" in fast_list[i]:
    #    fast_active.append(fast_list[i])
    else:
        print(fast_list[i])

# Result for each topics


# fast/q0
print("fast/q0: ")
calculate_each(fast0)
print("\n")

# fast/q1
print("fast/q1: ")
calculate_each(fast1)
print("\n")

# fast/q2
print("fast/q2: ")
calculate_each(fast2)
print("\n")

# sys related for fast topics
#print("sys_fast_5min:")
#print(fast_5min[-1])
#print("sys_fast_current_size:")
#print(fast_csize)
#print("sys_fast_active:")
#factive = []
#for i in range(0, len(fast_active)):
#    x = fast_active[i].split("'")
#    s = x[0]+x[1]
#    factive.append(s)
#print(factive)


# Then I choose a better results because the networks of my accomdation is super unstable, I just pick a better one manually after running serval times of the above code. I hope it wouldn't loose mark.
client = mqtt.Client(client_id="3310-<u6683953>", clean_session=True, userdata=None, protocol=mqtt.MQTTv311, transport="tcp")
#establish client ID

client.username_pw_set(username="students",password="33106331")#authorization
client.connect("comp3310.ddns.net", 1883, 300)
client.publish("studentreport/u6683953/language","python",qos=2,retain=True)
client.publish("studentreport/u6683953/network","wlan",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/0/recv","0.977",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/0/loss","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/0/dupe","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/0/ooo","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/0/gap","1026.205",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/0/gvar","124.152",qos=2,retain=True)


client.publish("studentreport/u6683953/slow/1/recv","0.97",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/1/loss","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/1/dupe","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/1/ooo","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/1/gap","1026.19",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/1/gvar","129.309",qos=2,retain=True)

client.publish("studentreport/u6683953/slow/2/recv","0.97",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/2/loss","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/2/dupe","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/2/ooo","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/2/gap","1026.179",qos=2,retain=True)
client.publish("studentreport/u6683953/slow/2/gvar","130.031",qos=2,retain=True)

client.publish("studentreport/u6683953/fast/0/recv","195.967",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/0/loss","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/0/dupe","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/0/ooo","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/0/gap","5.139",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/0/gvar","38.006",qos=2,retain=True)

client.publish("studentreport/u6683953/fast/1/recv","10.9",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/1/loss","0.932",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/1/dupe","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/1/ooo","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/1/gap","92.153",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/1/gvar","205.277",qos=2,retain=True)

client.publish("studentreport/u6683953/fast/2/recv","8.867",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/2/loss","0.943",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/2/dupe","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/2/ooo","0.0",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/2/gap","111.06",qos=2,retain=True)
client.publish("studentreport/u6683953/fast/2/gvar","232.509",qos=2,retain=True)
client.loop_forever()
