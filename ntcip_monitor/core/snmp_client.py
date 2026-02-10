from pysnmp.hlapi import (
    getCmd, setCmd, CommunityData, UdpTransportTarget,
    ContextData, ObjectType, ObjectIdentity, SnmpEngine,
    Integer32, Counter32, Unsigned32, Gauge32
)
import threading

class SNMPError(Exception):
    """Custom exception for SNMP errors."""
    pass

class EconoliteSNMPClient:
    """
    SNMP Client specifically tuned for Econolite/NTCIP controllers.
    Includes thread safety, chunking for large requests, and specific type handling.
    """
    
    def __init__(self, ip, port=501, community='administrator', timeout=2, retries=2):
        self.ip = ip
        self.port = port
        self.community = community
        self.timeout = timeout
        self.retries = retries
        self.engine = SnmpEngine()
        self._lock = threading.Lock()  # Thread safety lock
        
        self.stats = {
            'reads': 0,
            'writes': 0,
            'errors': 0
        }

    def get(self, *oids):
        """
        Perform SNMP GET on one or more OIDs.
        Includes Chunking (Size=1) to prevent 'Too Big' errors on Econolite controllers.
        Thread-safe.
        """
        with self._lock:
            self.stats['reads'] += 1
            
            # --- CHUNK SIZE CONFIGURATION ---
            # Set to 1 to force the controller to answer one item at a time.
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
                        raise SNMPError(f"SNMP Error: {errorIndication}")
                    
                    if errorStatus:
                        raise SNMPError(f"SNMP Error: {errorStatus.prettyPrint()} at index {errorIndex}")
                    
                    # Extract values from this chunk
                    chunk_values = [int(varBind[1]) for varBind in varBinds]
                    all_values.extend(chunk_values)
                
                # Return logic matches the original interface (list vs single item)
                return all_values if len(oids) > 1 else all_values[0]
                
            except StopIteration:
                self.stats['errors'] += 1
                raise SNMPError("No SNMP response received")
            except Exception as e:
                self.stats['errors'] += 1
                if isinstance(e, SNMPError):
                    raise e
                raise SNMPError(f"SNMP exception: {type(e).__name__}: {e}")

    def set(self, oid, value, asn_type=None):
        """
        Perform SNMP SET on a single OID.
        
        Args:
            oid (str): The OID to set.
            value (int/str): The value to set.
            asn_type (class, optional): specific PySNMP type (e.g., Counter32).
                                      If None, defaults to Integer32.
        """
        with self._lock:
            self.stats['writes'] += 1
            
            try:
                # Determine the correct ASN.1 type
                if asn_type:
                    val_object = asn_type(value)
                elif hasattr(value, 'prettyPrint'): 
                    # If it's already a PySNMP object, use it directly
                    val_object = value
                else:
                    # Default fallback
                    val_object = Integer32(value)

                iterator = setCmd(
                    self.engine,
                    CommunityData(self.community, mpModel=0),  # SNMPv1
                    UdpTransportTarget((self.ip, self.port), timeout=self.timeout, retries=self.retries),
                    ContextData(),
                    ObjectType(ObjectIdentity(oid), val_object)
                )
                
                errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
                
                if errorIndication:
                    raise SNMPError(f"SNMP SET Error: {errorIndication}")
                
                if errorStatus:
                    raise SNMPError(f"SNMP SET Error: {errorStatus.prettyPrint()} at index {errorIndex}")
                
                return True
                
            except Exception as e:
                self.stats['errors'] += 1
                raise SNMPError(f"SNMP SET exception: {e}")

    def get_stats(self):
        """Return connection statistics."""
        return self.stats