"""Real RTP packet parsing plus a basic marker-delimited payload collector."""
import hashlib
import struct

def parse(packet):
    if len(packet)<12:raise ValueError('short RTP packet')
    b0,b1,seq,timestamp,ssrc=struct.unpack('!BBHII',packet[:12])
    if b0!=0x80 or (b1&127)!=96:raise ValueError('unsupported RTP header')
    return {'sequence':seq,'timestamp':timestamp,'ssrc':ssrc,'marker':bool(b1&128),'payload':packet[12:]}

class Assembler:
    def build(self,packets,clock,first_sequence=None):
        groups={}
        for packet in packets:
            p=parse(packet);groups.setdefault((p['ssrc'],p['timestamp']),{})[p['sequence']]=p
        output=[]
        for (ssrc,timestamp),items in sorted(groups.items()):
            ordered=[items[i] for i in sorted(items)]
            # Prototype treats FU headers and ordinary NAL bytes alike.
            nal=b''.join(p['payload'] for p in ordered)
            output.append({'ssrc':ssrc,'timestamp':timestamp,'complete':any(p['marker'] for p in ordered),
                           'nal_hex':nal.hex(),'nal_sha256':hashlib.sha256(nal).hexdigest(),
                           'wallclock':clock.at(ssrc,timestamp)})
        return output
