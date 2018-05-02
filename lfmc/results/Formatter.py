from enum import Enum
import logging
import matplotlib.pyplot as plt
import matplotlib.animation as animation
plt.switch_backend('agg')

logging.basicConfig(filename='/var/log/lfmcserver.log', level=logging.DEBUG,
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
logger = logging.getLogger(__name__)


class Formatter(Enum):
    TIME_SERIES = 1
    MP4 = 2
    JSON = 3
    PDF = 4
    
    def describe(self):
        return self.name, self.value
    
    def __str__(self):
        return '{"FormatType": "' + str(self.value) + '"}'
    
    @classmethod
    async def format(cls, data, variable):
        switch = {
            1: cls.as_timeseries,
            2: cls.as_mp4,
            3: cls.as_json,
            4: cls.as_pdf
        }
        func = switch.get(cls.value)
        return func(data, variable)

    @staticmethod
    async def as_timeseries(data, variable):
        return '{"timeseries": ' + str(data) + ' }'

    @staticmethod
    async def as_mp4(data, variable):
        video_name = "/tmp/temp%s.mp4" % data[variable].name
        frames = []
        fig = plt.figure(figsize=(16, 9), dpi=96)
        plt.ylabel('latitude')
        plt.xlabel('longitude')
        plt.title(data.attrs["long_name"])
        logger.debug("\n--> Building MP4")

        ts = len(data["time"])
        for t in range(0, ts):
            b = data.isel(time=t)
            im = b[variable]
            plt.text(3, 1, "%s" % b["time"].values)
            frame = plt.imshow(im, cmap='viridis_r', animated=True)
            # Push onto array of frames
            frames.append([frame])
            logger.debug("\n--> Wrote frame %s of %s" % (t + 1, ts))

        vid = animation.ArtistAnimation(fig, frames, interval=50, blit=True, repeat_delay=1000)
        vid.save(video_name)
        logger.debug("\n--> Successfully wrote temp MP4 file.")
        return video_name

    @staticmethod
    async def as_json(data):
        return '{"json": ' + str(data) + ' }'
    


