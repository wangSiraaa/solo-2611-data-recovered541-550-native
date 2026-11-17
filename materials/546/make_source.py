"""Write finite real RTP and RTCP packets; opaque synthetic H264 NAL bytes."""
import struct

def rtp(seq,timestamp,payload,marker=True,ssrc=1):return struct.pack('!BBHII',0x80,96|(128 if marker else 0),seq,timestamp,ssrc)+payload
def sr(timestamp,seconds,fraction=0,ssrc=1):return struct.pack('!BBHIIIIII',0x80,200,6,ssrc,seconds,fraction,timestamp,0,0)

if __name__=='__main__':
    import json
    from pathlib import Path
    root=Path(__file__).parent/'fixtures';root.mkdir(exist_ok=True)
    packets={'single.rtp':rtp(1,90000,b'\x65ABCD'),'start.rtp':rtp(65535,90000,b'\x7c\x85AB',False),'end.rtp':rtp(0,90000,b'\x7c\x45CD'),
             'first.rtcp':sr(0,10),'last.rtcp':sr(180000,12)}
    for name,value in packets.items():(root/name).write_bytes(value)
    (root/'trace.json').write_text(json.dumps({'single':['single.rtp'],'fu_wrap':['end.rtp','start.rtp'],'reports':['first.rtcp','last.rtcp'],'first_sequence':{'single':{'1':1},'fu_wrap':{'1':65535}}},indent=2)+'\n')
