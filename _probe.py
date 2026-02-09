from pysnmp.hlapi import *

IP = '127.0.0.1'
PORT = 501
COMMUNITY = 'administrator'

# OIDs from your oid_definitions.py
# .5 = Don't Walk (Red), .6 = Ped Clear (Yellow), .7 = Walk (Green)
oids = {
    "Ped Walk (Ph 1-8)": "1.3.6.1.4.1.1206.4.2.1.1.4.1.7.1",
    "Ped Clear (Ph 1-8)": "1.3.6.1.4.1.1206.4.2.1.1.4.1.6.1",
    "Ped DW (Ph 1-8)":   "1.3.6.1.4.1.1206.4.2.1.1.4.1.5.1"
}

print(f"Probing {IP}...")
for name, oid in oids.items():
    print(f"Checking {name}...", end=" ")
    try:
        iterator = getCmd(SnmpEngine(), CommunityData(COMMUNITY, mpModel=0),
                          UdpTransportTarget((IP, PORT), timeout=1, retries=1),
                          ContextData(), ObjectType(ObjectIdentity(oid)))
        errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
        
        if errorIndication: print(f"ERROR: {errorIndication}")
        elif errorStatus:   print(f"MISSING: {errorStatus.prettyPrint()}")
        else:               print(f"FOUND! Value: {varBinds[0][1]}")
    except Exception as e:
        print(f"CRASH: {e}")