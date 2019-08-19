import cv2
import os
import numpy as np


class Video(object):
    def __init__(self, file_name):
        self.cap = cv2.VideoCapture(file_name)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.cap.release()

    @property
    def fps(self):
        return self.cap.get(cv2.CAP_PROP_FPS)

    @property
    def total_frames(self):
        return int(self.cap.get(cv2.CAP_PROP_FRAME_COUNT))

    @property
    def duration(self):
        return self.total_frames / self.fps

    def get_frame(self, frame_num):
        self.cap.set(cv2.CAP_PROP_POS_FRAMES, frame_num)
        success, img = self.cap.read()
        if not success:
            raise Exception('Error reading frame {} from video!'.format(frame_num))
        return img

    def get_x_frames_per_sec(self, x):
        if x > self.fps:
            raise ValueError('Desired FPS cannot be more than Video FPS. Desired FPS: {}, Video FPS: {}'.format(x, self.fps))

        for frame_num, img in self.get_x_frames_per_y_sec(x, 1):
            yield frame_num, img

    def get_x_frames_per_y_sec(self, x, y):
        if x > self.fps:
            raise ValueError('Desired FPS cannot be more than Video FPS. Desired FPS: {}, Video FPS: {}'.fprmat(x, self.fps))

        frame_step = self.fps * y / x
        for frame_num in np.arange(0, self.total_frames, frame_step):
            yield int(frame_num), self.get_frame(int(frame_num))

    def get_x_percent_frames(self, ratio):
        if ratio <= 0 or ratio > 1:
            raise ValueError('Ratio cannot be less than 0 or grater than 1, given: {}'.format(ratio))

        frame_step = 1 / ratio
        for frame_num in np.arange(0, self.total_frames, frame_step):
            yield int(frame_num), self.get_frame(int(frame_num))
