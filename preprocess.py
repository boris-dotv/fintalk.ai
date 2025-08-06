import os
import json
import shutil
from multiprocessing import Pool
from loguru import logger
from config import cfg
from file import load_pdf_info
from pdf_util import PdfExtractor
from financial_state import (extract_basic_info, extract_employee_info,
    extract_cbs_info, extract_cscf_info, extract_cis_info, extract_dev_info, merge_info)


def setup_xpdf():
    os.chdir(cfg.XPDF_PATH)  # 切换当前工作目录
    # xpdf/bin64/
    cmd = 'chmod +x pdftotext'
    os.system(cmd)  # 执行指令


def extract_pure_content(idx, key, pdf_path):
    logger.info('Extract text for {}:{}'.format(idx, key))
    save_dir = os.path.join(cfg.DATA_PATH, cfg.PDF_TEXT_DIR)
    # e.g. data/pdf_docs/
    key_dir = os.path.join(save_dir, key)
    if not os.path.exists(key_dir):
        os.mkdir(key_dir)
    save_path = os.path.join(key_dir, 'pure_content.txt')
    if os.path.exists(save_path):
        os.remove(save_path)
    PdfExtractor(pdf_path).extract_pure_content_and_save(save_path)


def extract_pdf_text(extract_func=extract_pure_content):
    setup_xpdf()  # 使得pdftotext可执行文件具有 +x 权限.

    save_dir = os.path.join(cfg.DATA_PATH, cfg.PDF_TEXT_DIR)
    # data/pdf_docs/
    if not os.path.exists(save_dir):
       os.mkdir(save_dir)

    pdf_info = load_pdf_info()

    with Pool(processes=cfg.NUM_PROCESSES) as pool:
        results = pool.starmap(extract_func, [(i, k, v['pdf_path']) for i, (k, v) in enumerate(pdf_info.items())])


def extract_pdf_tables():
    pdf_info = load_pdf_info()
    pdf_keys = list(pdf_info.keys())


    with Pool(processes=cfg.NUM_PROCESSES) as pool:
        results = pool.map(extract_basic_info, pdf_keys)
    merge_info('basic_info')

    with Pool(processes=cfg.NUM_PROCESSES) as pool:
        results = pool.map(extract_employee_info, pdf_keys)
    merge_info('employee_info')
    # cbs_info
    with Pool(processes=cfg.NUM_PROCESSES) as pool:
        results = pool.map(extract_cbs_info, pdf_keys)
    merge_info('cbs_info')
    # cscf_info
    with Pool(processes=cfg.NUM_PROCESSES) as pool:
        results = pool.map(extract_cscf_info, pdf_keys)
    merge_info('cscf_info')
    # cis_info
    with Pool(processes=cfg.NUM_PROCESSES) as pool:
        results = pool.map(extract_cis_info, pdf_keys)
    merge_info('cis_info')
    # dev_info
    with Pool(processes=cfg.NUM_PROCESSES) as pool:
        results = pool.map(extract_dev_info, pdf_keys)
    merge_info('dev_info')
