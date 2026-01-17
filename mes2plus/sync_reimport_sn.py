from __future__ import annotations

import contextlib
from datetime import datetime, timedelta
import logging
import os
import re
from typing import Generator, Optional
from zoneinfo import ZoneInfo

from dotenv import load_dotenv
import pymysql

load_dotenv()

type PyMysqlConn = pymysql.Connection[pymysql.cursors.DictCursor]


def get_mes_conn(autocommit: bool = True) -> PyMysqlConn:
    """获取 MES 数据库连接"""
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_MES_HOST"),
            port=int(os.getenv("DB_MES_PORT", 3306)),
            user=os.getenv("DB_MES_USER"),
            password=os.getenv("DB_MES_PASS", ""),
            database=os.getenv("DB_MES_NAME"),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=autocommit,
        )
        return conn
    except Exception as e:
        logging.error(f"连接 MES 数据库失败: {e}")
        raise e


def get_plus_conn(autocommit: bool = True) -> PyMysqlConn:
    """获取 PLUS 数据库连接"""
    try:
        conn = pymysql.connect(
            host=os.getenv("DB_PLUS_HOST"),
            port=int(os.getenv("DB_PLUS_PORT", 3306)),
            user=os.getenv("DB_PLUS_USER"),
            password=os.getenv("DB_PLUS_PASS", ""),
            database=os.getenv("DB_PLUS_NAME"),
            charset="utf8mb4",
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=autocommit,
        )
        return conn
    except Exception as e:
        logging.error(f"连接 PLUS 数据库失败: {e}")
        raise e


def query_mes_recently_barcode_creations(conn: PyMysqlConn) -> list[dict]:
    """获取近期更新的条码生成记录"""
    sql = """
    SELECT
        t1.bc_id,
        t3.task_code,
        t1.inv_code,
        t1.inv_name,
        t3.order_code
    FROM
        `jgmes_barcode_create` AS t1
        LEFT JOIN `jgmes_modeling_inventory` AS t2 ON t2.inv_code = t1.inv_code
        LEFT JOIN `jgmes_pm_production_task` AS t3 ON t3.task_code = t1.bill_code
    WHERE
        t1.last_update_date BETWEEN %s AND %s
        AND t2.ic_id = 270
    ORDER BY
        t1.last_update_date DESC
    """
    start_date = datetime.now(ZoneInfo("Asia/Shanghai")) - timedelta(days=2)
    stop_date = datetime.now(ZoneInfo("Asia/Shanghai")) + timedelta(days=1)

    with conn.cursor() as cursor:
        cursor.execute(sql, (start_date, stop_date))
        return list(cursor.fetchall())


def query_mes_barcode_creation_barcodes(conn: PyMysqlConn, bc_id: int) -> list[dict]:
    """查询条码生成记录下的条码列表"""
    sql = """
    SELECT
        bd_id,
        `code`
    FROM
        jgmes_barcode_data
    WHERE
        bc_id = %s
        AND delete_flag = 0        
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (bc_id,))
        return list(cursor.fetchall())


def __extract_order_number(order: str) -> str:
    """从输入字符串中提取正确的订单号"""

    # 第一优先级：匹配带有 -数字-数字 后缀的订单号，只取主体部分
    pattern_with_suffix = r"[A-Z]+[A-Z0-9-]*(?=-[0-9]-[0-9])"
    match = re.search(pattern_with_suffix, order)
    if match:
        return match.group(0)

    # 第二优先级：普通订单号（无特定后缀）
    pattern_normal = r"[A-Z]+[A-Z0-9-]*"
    match = re.search(pattern_normal, order)
    if match:
        return match.group(0)

    return order


def query_plus_imported_barcodes(conn: PyMysqlConn, order_code: str) -> list[dict]:
    """查询 PLUS 已导入的条码列表"""
    sql = """
    SELECT
        `SN码` AS `code`
    FROM
        `物料扫码-SN库`
    WHERE
        `销售订单` = %s;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (order_code,))
        return list(cursor.fetchall())


def query_plus_incoming_barcodes(conn: PyMysqlConn, order_code: str) -> list[dict]:
    """查询 PLUS 已入库的条码列表"""
    sql = """
    SELECT
        `SN码` AS `code`
    FROM
        `物料扫码-库存`
    WHERE
        `销售订单` = %s;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (order_code,))
        return list(cursor.fetchall())


def query_plus_outgoing_barcodes(conn: PyMysqlConn, order_code: str) -> list[dict]:
    """查询 PLUS 已出库的条码列表"""
    sql = """
    SELECT
        `SN码` AS `code`
    FROM
        `物料扫码-出库`
    WHERE
        `销售订单` = %s;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (order_code,))
        return list(cursor.fetchall())


def delete_plus_imported_barcodes(conn: PyMysqlConn, order_code: str) -> int:
    """删除 PLUS 已导入的条码列表"""
    sql = """
    DELETE FROM
        `物料扫码-SN库`
    WHERE
        `销售订单` = %s;
    """
    with conn.cursor() as cursor:
        cursor.execute(sql, (order_code,))
        return cursor.rowcount


def insert_plus_barcodes(
    conn: PyMysqlConn, order_code: str, item_code: str, barcodes: list[dict]
) -> int:
    """插入 PLUS 条码列表"""
    sql = """
    INSERT INTO `物料扫码-SN库` (`销售订单`, `物料编码`, `SN码`, `导入来源`, `录入人`, `录入时间`)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    now = datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S")
    params = [
        (order_code, item_code, barcode["code"], "机器人", "机器人", now)
        for barcode in barcodes
    ]
    with conn.cursor() as cursor:
        cursor.executemany(sql, params)
        return cursor.rowcount


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    load_dotenv()
    logging.info("开始执行")

    mes_conn: Optional[PyMysqlConn] = None
    plus_conn: Optional[PyMysqlConn] = None
    try:
        # 连接数据库
        mes_conn = get_mes_conn()
        logging.info("连接 MES 数据库成功")
        plus_conn = get_plus_conn()
        logging.info("连接 PLUS 数据库成功")

        # 获取近期更新的条码生成记录
        barcode_creations = query_mes_recently_barcode_creations(mes_conn)
        logging.info(f"近期更新的条码生成记录: {len(barcode_creations)} 条")

        # 遍历近期更新的条码生成记录
        for barcode_creation in barcode_creations:
            order_code = __extract_order_number(barcode_creation["order_code"])
            logging.info(f"正在处理: {barcode_creation['task_code']} - {order_code}")

            # 查询导入的条码列表
            mes_barcodes = query_mes_barcode_creation_barcodes(
                mes_conn, barcode_creation["bc_id"]
            )
            mes_codes = [code["code"] for code in mes_barcodes]
            logging.info(f"MES 导入的条码列表: {len(mes_codes)} 条")

            # 查询 PLUS 已导入的条码列表
            plus_barcodes = query_plus_imported_barcodes(plus_conn, order_code)
            plus_codes = [code["code"] for code in plus_barcodes]
            logging.info(f"PLUS 已导入的条码列表: {len(plus_codes)} 条")

            # 检查数量是否一致，不一致则跳过
            if len(mes_codes) != len(plus_codes):
                logging.warning("两边的条码数量不一致，跳过不处理")
                continue

            # 检查两边的条码是否一致，一致则跳过
            if set(mes_codes) == set(plus_codes):
                logging.warning("两边的条码内容一致，跳过不处理")
                continue

            # 查询 PLUS 已入/出库的条码列表
            plus_incoming_barcodes = query_plus_incoming_barcodes(plus_conn, order_code)
            plus_outgoing_barcodes = query_plus_outgoing_barcodes(plus_conn, order_code)
            if plus_incoming_barcodes or plus_outgoing_barcodes:
                logging.warning("已存在已入/出库的条码，跳过不处理")
                continue

            # 更新 PLUS 已导入的条码
            plus_txn_conn = get_plus_conn(autocommit=False)
            try:
                # 删除 PLUS 已导入的条码
                delete_count = delete_plus_imported_barcodes(plus_txn_conn, order_code)
                logging.info(f"删除 PLUS 已导入的条码: {delete_count} 条")

                # 插入 PLUS 条码列表
                insert_count = insert_plus_barcodes(
                    plus_txn_conn,
                    order_code,
                    barcode_creation["inv_code"],
                    mes_barcodes,
                )
                logging.info(f"插入 PLUS 条码列表: {insert_count} 条")

                plus_txn_conn.commit()
                logging.info("重新导入成功")
            except Exception as e:
                logging.error(f"重新导入失败，回退事务: {e}")
                raise e
            finally:
                if plus_txn_conn:
                    plus_txn_conn.close()

    except Exception as e:
        logging.error(f"处理失败: {e}")
        raise e
    finally:
        if mes_conn:
            mes_conn.close()
        if plus_conn:
            plus_conn.close()
