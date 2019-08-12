# -*- coding: utf-8 -*-
'''
wsi image crop module
'''
from __future__ import print_function

#pylint:disable=import-error,ungrouped-imports
import os
import io
import time
import array
import threading
import multiprocessing as mp
from io import BytesIO
from queue import Queue
from configparser import ConfigParser
import numpy as np
from PIL import Image
from resource_util import get_cpu_state, get_memory_state
import logging

#pylint:disable=no-name-in-module
import openslide
# from util.common import logging
from kfb_read import open_kfb, close_kfb, get_header, read_region

PWD, _ = os.path.split(__file__)
CROP_CP = ConfigParser()
CROP_CP.read(os.path.join(PWD, 'crop_config.ini'))
PATCH_WIDTH = CROP_CP['CONFIG'].getfloat('PATCH_WIDTH')
PATCH_HEIGHT = CROP_CP['CONFIG'].getfloat('PATCH_HEIGHT')
PROCESS_MODE = CROP_CP['CONFIG'].get('process_mode')
THREAD_NUM = CROP_CP['CONFIG'].getint('thread_num')
TIME_OUT = CROP_CP['CONFIG'].getint('TIME_OUT')
CONNECT_RETRY_NUM = CROP_CP['CONFIG'].getint('CONNECT_RETRY_NUM')
logging.info("Patch_width:%.2f, Patch_height:%.2f, Process_mode:%s", PATCH_WIDTH, PATCH_HEIGHT, PROCESS_MODE)


#pylint:disable=too-many-arguments
def crop_thread(pid, file_path, worker_num, target_size, save_path, que,
                finish_tag):
    '''
    crop by thread
    :param pid: paralle id
    :param file_path: wsi file path
    :param worker_num: worker num
    :param target_size: crop target size
    :param save_path: images will be saved if save_path is set
    :param que: thread queue
    :param finish_tag: finish_tag list
    :return: None
    '''
    slice_dict = crop_task(pid, file_path, worker_num, target_size, save_path)
    que.put(slice_dict)
    finish_tag[pid] = 1 if slice_dict else 0

def wsi_info(file_path):
    '''
    get wsi images info, include image_height,
    image_width, x_mpp, y_mpp
    :param file_path:
    :return:
    '''
    if 'kfb' in file_path:
        slide = open_kfb(file_path)
        image_height, image_width, _, _, _, \
        image_cap_res, _ = get_header(slide)
        x_mpp, y_mpp = image_cap_res, image_cap_res
        close_kfb(slide)
    elif 'svs' in file_path or 'ndpi' in file_path:
        slide = openslide.OpenSlide(file_path)
        x_mpp = float(slide.properties[openslide.PROPERTY_NAME_MPP_X])
        y_mpp = float(slide.properties[openslide.PROPERTY_NAME_MPP_Y])
        image_width, image_height = slide.level_dimensions[0]
        slide.close()
    else:
        raise TypeError()

    return dict(
        image_height=image_height,
        image_width=image_width,
        x_mpp=x_mpp,
        y_mpp=y_mpp
    )


# pylint:disable=too-many-locals
def crop_task(pid, file_path, worker_num, target_size=None, save_path=None, mgrdict=None):
    """
    crop process task
    :param pid:process id
    :param file_path:wsi images file
    :param worker_num:total worker num
    :param target_size:size for images
    :param save_path:if is set, croped images will save
    :param mgrdict:manager.dict
    """

    _, filename = os.path.split(file_path)
    if 'kfb' in file_path:
        slide = open_kfb(file_path)
        image_height, image_width, scan_scale, _, _, \
        image_cap_res, _ = get_header(slide)
        x_mpp, y_mpp = image_cap_res, image_cap_res
        #pylint:disable=invalid-name
        def wsi_read(x, y, width, height):
            #pylint:disable=fixme
            #todo: optimize kfb read_region procedure, cost 20 sec
            region = read_region(slide, scan_scale, width * x, height * y, width, height)
            kfb_image_array = array.array('B', region[0])
            if save_path:
                patch_name = '{}_{}_{}'.format(filename, x * width, y * height)
                with open('{}/{}.jpg'.format(save_path, patch_name), 'wb') as kfb_fp:
                    kfb_fp.write(kfb_image_array)
            return Image.open(BytesIO(kfb_image_array))
        def close():
            close_kfb(slide)

    elif 'svs' in file_path or 'ndpi' in file_path:
        slide = openslide.OpenSlide(file_path)
        x_mpp = float(slide.properties[openslide.PROPERTY_NAME_MPP_X])
        y_mpp = float(slide.properties[openslide.PROPERTY_NAME_MPP_Y])
        image_width, image_height = slide.level_dimensions[0]
        # pylint:disable=invalid-name
        def wsi_read(x, y, width, height):
            image = slide.read_region((width * x, height * y), 0, (width, height)).convert('RGB')
            #image need to change to jpeg
            buffer = io.BytesIO()
            image.save(buffer, format="JPEG")
            image = Image.open(buffer)

            if save_path:
                patch_name = '{}_{}_{}'.format(filename, x * width, y * height)
                image.save('{}/{}.jpg'.format(save_path, patch_name), 'jpeg')

            return image

        def close():
            slide.close()
    else:
        raise TypeError()

    width = int(PATCH_WIDTH / x_mpp)
    height = int(PATCH_HEIGHT / y_mpp)
    num_width = int(image_width / width)
    num_height = int(image_height / height)
    task = np.arange(num_width * num_height).reshape(num_width, num_height)
    i_rank, j_rank = np.where(task % worker_num == pid)

    results_dict = mgrdict if mgrdict is not None else {}
    for i, j in zip(i_rank, j_rank):
        img = wsi_read(i, j, width, height)
        # do not change this key, because globle coordidate will
        # extract from this key
        patch_name = '{}_{}_{}'.format(filename, i * width, j * height)
        if target_size:
            img = img.resize(target_size)
        results_dict[patch_name] = img
    # mgrdict.update(imgs_dict)
    close()
    if mgrdict is None:
        return results_dict
    return None


# pylint:disable=too-few-public-methods
class CropProcess():
    '''
    CropProcess
    '''
    def __init__(self, worker_num):
        '''
        CropProcess
        :param worker_num:
        '''
        self.worker_num = worker_num

    def crop(self, file_path, save_path=None):
        '''
        crop
        :param file_path:
        :param save_path:
        :return:
        '''
        return self.wsi_crop(file_path, self.worker_num, save_path=save_path)


    #pylint:disable=too-many-branches, too-many-statements
    def wsi_crop(self, file_path, worker_num, target_size=None, save_path=None):
        '''
        wsi image crop function
        :param file_path:
        :param worker_num:
        :param target_size:
        :param save_path:
        :return:
        '''
        start = time.time()
        filename = os.path.split(file_path)[1]
        if save_path:
            save_folder = os.path.join(save_path, filename)
            if not os.path.isdir(save_folder):
                os.mkdir(save_folder)
        else:
            save_folder = None
        if PROCESS_MODE == "auto":
            mode = "process" if 'kfb' in file_path else "thread"
        else:
            mode = PROCESS_MODE

        imgs_dict = {}
        if mode == 'process':
            #multiprocess crop_task
            manager_list = [mp.Manager() for _ in range(worker_num)]
            mgr_dict_list = [manager.dict() for manager in manager_list]
            #return will cost 20sec
            p_list = [mp.Process(target=crop_task,
                                 args=(i, file_path, worker_num, target_size,
                                       save_folder, mgr_dict_list[i])) for i in range(worker_num)]
            for process in p_list:
                process.start()
            for i, process in enumerate(p_list):
                process.join(600)
            logging.info(get_memory_state())
            logging.info(get_cpu_state())

            for mgr_dict in mgr_dict_list:
                if not mgr_dict:
                    raise Exception("read process error")
            for i in range(worker_num):
                connect_count = 0
                while True:
                    try:
                        imgs_dict.update(dict(mgr_dict_list[i]))
                        manager_list[i].shutdown()
                        p_list[i].terminate()
                        break
                    except ConnectionError:
                        if connect_count < CONNECT_RETRY_NUM:
                            connect_count += 1
                            logging.warn("Connection error, retry %d", connect_count)
                            time.sleep(0.01)
                        else:
                            raise ConnectionError("Can not get data from worker %d, retry fail", i)
                    except Exception as exc:
                        logging.error("Can not get data from worker %d", i)
                        raise exc

        else:
            que = Queue()
            finish_tag = {}

            # paral_num = worker_num
            paral_num = THREAD_NUM

            threads = [threading.Thread(target=crop_thread,
                                        args=(i, file_path, paral_num, target_size,
                                              save_folder, que, finish_tag))
                       for i in range(paral_num)]
            for thread in threads:
                thread.setDaemon(True)
                thread.start()
            read_start_time = time.time()
            while not (que.empty() and len(finish_tag) == paral_num):
                if not que.empty():
                    imgs_dict.update(que.get())
                else:
                    read_now_time = time.time()
                    if read_now_time-read_start_time > TIME_OUT:
                        raise TimeoutError("read TimeOut")
                    time.sleep(0.1)


        logging.info('Read time = {}'.format(time.time() - start))
        image_info = wsi_info(file_path)
        logging.info("%s info:%s", filename, str(image_info))
        return imgs_dict, image_info
