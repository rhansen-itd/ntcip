"""
SNMP Client for NTCIP Communication with Econolite Controllers
"""

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
        
        Args:
            ip: Controller IP address
            port: SNMP port (default 501 for Econolite)
            community: Community string / username (default 'administrator')
            timeout: SNMP timeout in seconds
            retries: Number of retries
        """
        self.ip = ip
        self.port = port
        self.community = community
        self.timeout = timeout
        self.retries = retries
        self.engine = SnmpEngine()
        
        # Stats
        self.stats = {
            'reads': 0,
            'writes': 0,
            'errors': 0
        }
    
    def get(self, *oids):
        """
        Perform SNMP GET on one or more OIDs.
        
        Args:
            *oids: Variable number of OID strings
        
        Returns:
            list: List of integer values, or None on error
        """
        self.stats['reads'] += 1
        
        # Build OID list
        oid_objects = [ObjectType(ObjectIdentity(oid)) for oid in oids]
        
        try:
            iterator = getCmd(
                self.engine,
                CommunityData(self.community, mpModel=0),  # SNMPv1
                UdpTransportTarget((self.ip, self.port), timeout=self.timeout, retries=self.retries),
                ContextData(),
                *oid_objects
            )
            
            errorIndication, errorStatus, errorIndex, varBinds = next(iterator)
            
            if errorIndication:
                self.stats['errors'] += 1
                raise SNMPError(f"SNMP Error: {errorIndication}")
            
            if errorStatus:
                self.stats['errors'] += 1
                raise SNMPError(f"SNMP Error: {errorStatus.prettyPrint()} at index {errorIndex}")
            
            # Extract values
            values = [int(varBind[1]) for varBind in varBinds]
            
            return values if len(values) > 1 else values[0]
            
        except StopIteration:
            self.stats['errors'] += 1
            raise SNMPError("No SNMP response received")
        except Exception as e:
            self.stats['errors'] += 1
            raise SNMPError(f"SNMP exception: {type(e).__name__}: {e}")
    
    def set(self, oid, value):
        """
        Perform SNMP SET on a single OID.
        
        Args:
            oid: OID string
            value: Integer value to set
        
        Returns:
            bool: True on success, raises SNMPError on failure
        """
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
        """Get client statistics."""
        return self.stats.copy()
    
    def test_connection(self):
        """
        Test SNMP connectivity.
        
        Returns:
            bool: True if connection successful
        """
        try:
            # Try to read sysDescr (universal OID)
            self.get('1.3.6.1.2.1.1.1.0')
            return True
        except SNMPError:
            return False


class SNMPError(Exception):
    """Custom exception for SNMP errors."""
    pass
