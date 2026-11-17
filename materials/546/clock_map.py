"""Small RTCP-SR time product, initially uses the nominal first-anchor rule."""
import struct
from fractions import Fraction

def sender_report(packet):
    version,count,pt,length=packet[0]>>6,packet[0]&31,packet[1],struct.unpack('!H',packet[2:4])[0]
    if version!=2 or count!=0 or pt!=200 or length!=6 or len(packet)!=28:raise ValueError('unsupported sender report')
    ssrc,sec,frac,rtp,packets,octets=struct.unpack('!IIIIII',packet[4:])
    return {'ssrc':ssrc,'rtp':rtp,'ntp':Fraction(sec*(1<<32)+frac,1<<32)}

class Clock:
    def __init__(self,reports,rate=90000):
        self.rate=rate
        self.anchors=[sender_report(p) for p in reports]
        self.anchors.sort(key=lambda a:a['rtp'])
    def at(self,ssrc,timestamp):
        first=next(a for a in self.anchors if a['ssrc']==ssrc)
        value=first['ntp']+Fraction(timestamp-first['rtp'],self.rate)
        return {'ssrc':ssrc,'rtp_timestamp':timestamp,'ntp_numerator':value.numerator,'ntp_denominator':value.denominator}
