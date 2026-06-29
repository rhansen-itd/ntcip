# record.py
from video_buffer import VideoBufferConfig, VideoBufferManager
import cv2,time

rtsp_feed="rtsp://root:WesternSystems1!@10.37.23.204/axis-media/media.amp?camera=1&videoCodec=h264"

cap = cv2.VideoCapture(rtsp_feed)
start = time.time()
count = 0

while time.time() - start < 10:
    ret, _ = cap.read()
    if ret:
        count += 1

cap.release()
print(f"Actual delivered FPS: {count / 10:.1f}")

config = VideoBufferConfig(
    streams={"cam1": rtsp_feed},
    trigger_dir="./trigger_queue",
    output_dir="./completed_videos",
    pre_roll_sec=0.0,
)

manager = VideoBufferManager(config)
manager.start()  # blocks