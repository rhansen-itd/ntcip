"""
SNMP Client for NTCIP Communication with Econolite Controllers
"""

import threading
import warnings
warnings.filterwarnings('ignore', message='.*pysnmp.*deprecated.*')

from pysnmp.hlapi import (
    getCmd, setCmd, CommunityData, UdpTransportTarget,
    ContextData, ObjectType, ObjectIdentity, SnmpEngine, Integer32
)


class EconoliteSNMPClient:
    """
    SNMP client optimized for Econolite Cobalt/EOS controllers.
    
    Econolite Specifics:
    - Port 501 (not standard 161)
    - SNMP v1 (not v2c)
    - Community string = controller username
    """
    
    def __init__(self, ip, port=501, community='administrator', timeout=2, retries=2):
        """
        Initialize SNMP client.
        """
        self.ip = ip
        self.port = port
        self.community = community
        self.timeout = timeout
        self.retries = retries
        self.engine = SnmpEngine()
        self._lock = threading.Lock()
        
        # Stats
        self.stats = {
            'reads': 0,
            'writes': 0,
            'errors': 0
        }
    
    def get(self, *oids):
        """
        Perform SNMP GET on one or more OIDs.
        
        Updated with Chunking to prevent 'Too Big' errors on Econolite controllers.
        """
        
        with self._lock:
            self.stats['reads'] += 1
            
            # --- CHUNK SIZE CONFIGURATION ---
            # Set to 1 to force the controller to answer one item at a time.
            # This is the safest way to fix the missing Outputs/Detectors.
            CHUNK_SIZE = 1
            
            all_values = []
            
            # Convert all OID strings to ObjectType objects once
            all_oid_objects = [ObjectType(ObjectIdentity(oid)) for oid in oids]
            
            try:
                # Process the OIDs in small chunks
                for i in range(0, len(all_oid_objects), CHUNK_SIZE):
                    chunk = all_oid_objects[i:i + CHUNK_SIZE]
                    
                    iterator = getCmd(
                        self.engine,
                        CommunityData(self.community, mpModel=0),  # SNMPv1
                        UdpTransportTarget((self.ip, self.port), timeout=self.timeout, retries=self.retries),
                        ContextData(),
                        *chunk
                    )
                    
                    errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
                    
                    if errorIndication:
                        self.stats['errors'] += 1
                        raise SNMPError(f"SNMP Error: {errorIndication}")
                    
                    if errorStatus:
                        self.stats['errors'] += 1
                        raise SNMPError(f"SNMP Error: {errorStatus.prettyPrint()} at index {errorIndex}")
                    
                    # Extract values from this chunk and add to our master list
                    chunk_values = [int(varBind[1]) for varBind in varBinds]
                    all_values.extend(chunk_values)
                
                # Return logic matches the original interface (list vs single item)
                return all_values if len(oids) > 1 else all_values[0]
                
            except StopIteration:
                self.stats['errors'] += 1
                raise SNMPError("No SNMP response received")
            except Exception as e:
                self.stats['errors'] += 1
                # Re-raise known SNMP errors to keep error handling consistent
                if isinstance(e, SNMPError):
                    raise e
                raise SNMPError(f"SNMP exception: {type(e).__name__}: {e}")
    
    def set(self, oid, value):
        """
        Perform SNMP SET on a single OID.
        """
        with self._lock:
            self.stats['writes'] += 1
            
            try:
                iterator = setCmd(
                    self.engine,
                    CommunityData(self.community, mpModel=0),  # SNMPv1
                    UdpTransportTarget((self.ip, self.port), timeout=self.timeout, retries=self.retries),
                    ContextData(),
                    ObjectType(ObjectIdentity(oid), Integer32(value))
                )
                
                errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
                
                if errorIndication:
                    self.stats['errors'] += 1
                    raise SNMPError(f"SNMP SET Error: {errorIndication}")
                
                if errorStatus:
                    self.stats['errors'] += 1
                    raise SNMPError(f"SNMP SET Error: {errorStatus.prettyPrint()} at index {errorIndex}")
                
                return True
                
            except StopIteration:
                self.stats['errors'] += 1
                raise SNMPError("No SNMP response to SET command")
            except Exception as e:
                self.stats['errors'] += 1
                raise SNMPError(f"SNMP SET exception: {type(e).__name__}: {e}")
    
    def get_stats(self):
        return self.stats.copy()
    
    def test_connection(self):
        try:
            self.get('1.3.6.1.2.1.1.1.0')
            return True
        except SNMPError:
            return False


class SNMPError(Exception):
    pass