# RTP与RTCP采集数据起点

`python3 make_source.py` 可重建 fixtures 中真实标准头的 RTP v2 / RTCP Sender Report 二进制文件；`python3 demo.py` 运行普通单NAL正常与FU-A序号回绕例。NAL正文为本地虚构不透明字节，不是可播放视频，不含患者或真实通信数据。任务只重组压缩access-unit字节与时间事实，不要求H264解码器。clock_map.py 对独立客户提供SR映射；access_units.py读取RTP生产NAL/AU。当前首锚+标称时钟与按原始序号拼负载是简化起点，不是目标答案。
