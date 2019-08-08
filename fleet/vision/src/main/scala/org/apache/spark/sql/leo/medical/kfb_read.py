# -*- coding: utf-8 -*-
'''
kfb tool kit
'''
from __future__ import division, print_function

#pylint:disable=unused-variable, unused-wildcard-import, wildcard-import
from ctypes import *

import os

class OpenError(Exception):
    """No such file or does not support the requested file.

    """

class CloseError(Exception):
    """   image cannot close
    """

class LoadModelError(Exception):
    """   Load the wrong lib model

    """

# pylint:disable=too-few-public-methods, invalid-name
class _utf8_p():
    """Wrapper class to convert string arguments to bytes."""

    _bytes_type = bytes
    _str_type = str

    @classmethod
    def from_param(cls, obj):
        """from_param"""
        if isinstance(obj, cls._bytes_type):
            return obj
        if isinstance(obj, cls._str_type):
            return obj.encode('UTF-8')
        raise TypeError('Incorrect type')

ROOT_PATH, _ = os.path.split(__file__)
LIB_PATH = os.path.join(ROOT_PATH, 'libImageOperationLib.so')
LIB = CDLL(LIB_PATH, mode=RTLD_GLOBAL)


# pylint:disable=too-few-public-methods
class ImageInfo(Structure):
    """ ImageInfo """
    _fields_ = [('DataFilePTR', c_int)]


class KfbList():
    """KfbList"""
    def __init__(self, buffer, length):
        self.buffer = buffer
        self.length = length

    def __len__(self):
        return self.length

    def __getitem__(self, idx):
        if idx < self.length:
            return self.buffer[idx]
        raise IndexError("index {} is out of bonds!".format(idx))


def open_kfb(kfb_path):
    """open_kfb"""
    if LIB.InitImageFileFunc is None:
        raise LoadModelError("Load the wrong lib model")
    LIB.InitImageFileFunc.restype = c_int
    LIB.InitImageFileFunc.argtypes = [POINTER(ImageInfo), _utf8_p]
    data_file_ptr = c_int(1)
    image_point = ImageInfo()
    #pylint:disable=invalid-name,attribute-defined-outside-init
    image_point.DataFilePTR = data_file_ptr
    res = LIB.InitImageFileFunc(byref(image_point), kfb_path)
    # print(image_point.DataFilePTR)
    if res == 0:
        print(kfb_path)
        raise OpenError("No such file or does not support the requested file")
    return image_point


def close_kfb(image_point):
    """close_kfb"""
    if LIB.UnInitImageFileFunc is None:
        raise LoadModelError("Load the wrong lib model")
    LIB.UnInitImageFileFunc.restype = c_int
    LIB.UnInitImageFileFunc.argtypes = [POINTER(ImageInfo)]
    res = LIB.UnInitImageFileFunc(byref(image_point))
    if res == 0:
        raise CloseError("image cannot close")
    return image_point


def get_header(image_point):
    """get_header"""
    if LIB.GetHeaderInfoFunc is None:
        raise LoadModelError("Load the wrong lib model")
    LIB.GetHeaderInfoFunc.restype = c_int
    LIB.GetHeaderInfoFunc.argtypes = [POINTER(ImageInfo),
                                      POINTER(c_int),
                                      POINTER(c_int),
                                      POINTER(c_int),
                                      POINTER(c_float),
                                      POINTER(c_double),
                                      POINTER(c_float),
                                      POINTER(c_int)]
    khi_image_height = c_int(0)
    khi_image_width = c_int(0)
    khi_scan_scale = c_int(0)
    khi_spend_time = c_float(0)
    khi_scan_time = c_double(0)
    khi_image_cap_res = c_float(0)
    khi_image_block_size = c_int(0)
    res = LIB.GetHeaderInfoFunc(pointer(image_point),
                                pointer(khi_image_height),
                                pointer(khi_image_width),
                                pointer(khi_scan_scale),
                                pointer(khi_spend_time),
                                pointer(khi_scan_time),
                                pointer(khi_image_cap_res),
                                pointer(khi_image_block_size))
    if res == 0:
        raise Exception("cannot get Image header")
    return khi_image_height.value, \
           khi_image_width.value, \
           khi_scan_scale.value, \
           khi_spend_time.value, \
           khi_scan_time.value, \
           khi_image_cap_res.value, \
           khi_image_block_size.value


def get_stream(image_point, fscale, n_image_posx, n_image_posy):
    """get_stream"""
    if LIB.GetImageStreamFunc is None:
        raise Exception("Cannot get image stream")
    LIB.GetHeaderInfoFunc.restype = c_char_p
    LIB.GetHeaderInfoFunc.argtypes = [POINTER(ImageInfo), c_float, c_int, c_int,
                                      POINTER(c_int), POINTER(POINTER(c_ubyte))]
    image_data = POINTER(c_ubyte)()
    n_data_length = c_int(4)
    LIB.GetImageStreamFunc(pointer(image_point),
                           c_float(fscale),
                           c_int(n_image_posx),
                           c_int(n_image_posy),
                           pointer(n_data_length),
                           pointer(image_data))

    return [image_data, n_data_length.value]


def get_thumnail(image_point):
    '''
    get_thumnail
    :param image_point: image_point
    :return:
    '''
    if LIB.GetThumnailImageFunc is None:
        raise Exception('none get thumnial function')
    LIB.GetThumnailImageFunc.restype = c_int
    LIB.GetThumnailImageFunc.argtypes = [POINTER(ImageInfo), POINTER(POINTER(c_ubyte)),
                                         POINTER(c_int), POINTER(c_int), POINTER(c_int)]
    image_data = POINTER(c_ubyte)()
    ndata_length = c_int(1)
    nthum_width = c_int(1)
    nthum_height = c_int(1)
    LIB.GetThumnailImageFunc(pointer(image_point), pointer(image_data), pointer(ndata_length),
                             pointer(nthum_width), pointer(nthum_height))

    image_data_list = [image_data[i] for i in range(ndata_length.value)]
    return image_data_list, ndata_length.value, nthum_width.value, nthum_width.value


# pylint:disable=too-many-arguments
def read_region(image_point, fscale, sp_x, sp_y, nwidth, nheight):
    """read_region"""
    if LIB.GetImageDataRoiFunc is None:
        raise Exception('none GetImageDataRoiFunc function')
    LIB.GetImageDataRoiFunc.restype = c_int
    LIB.GetImageDataRoiFunc.argtypes = [POINTER(ImageInfo), c_float, c_int, c_int, c_int, c_int,
                                        POINTER(POINTER(c_ubyte)), POINTER(c_int), POINTER(c_bool)]
    p_buffer = POINTER(c_ubyte)(c_byte(1))
    data_length = c_int()
    flag = c_bool(False)
    LIB.GetImageDataRoiFunc(pointer(image_point), c_float(fscale), c_int(sp_x),
                            c_int(sp_y), c_int(nwidth), c_int(nheight),
                            pointer(p_buffer), pointer(data_length), pointer(flag))
    region_list = KfbList(p_buffer, data_length.value)
    return [region_list, data_length.value, flag.value]


def get_rgb_stream(image_point, fscale, sp_x, sp_y):
    """get_rgb_stream"""
    if LIB.GetImageRGBDataStreamFunc is None:
        raise Exception('none GetImageDataRoiFunc function')
    LIB.GetImageRGBDataStreamFunc.restype = POINTER(c_byte)
    LIB.GetImageRGBDataStreamFunc.argtypes = [POINTER(ImageInfo), c_float, c_int, c_int,
                                              POINTER(c_int), POINTER(c_int), POINTER(c_int),
                                              POINTER(POINTER(c_ubyte))]
    p_buffer = POINTER(c_ubyte)()
    data_length = c_int()
    width = c_int()
    height = c_int()
    LIB.GetImageRGBDataStreamFunc(
        pointer(image_point), c_float(fscale), c_int(sp_x), c_int(sp_y),
        pointer(data_length), pointer(width), pointer(height),
        pointer(p_buffer))
    return p_buffer, data_length.value
