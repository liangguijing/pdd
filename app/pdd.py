import asyncio
import datetime
import traceback
from app import settings
from api.erp321 import Erp321
from api.jdy_v5 import JdyV5
from api.pdd import Pdd
from app.utils import err_handler
from datetime import timedelta, datetime as dt
from logger import get_logger
from models.pdd import Session, Monitor, Order, Item, Mall


"""
定时任务执行, 同步拼多多订单到本地数据库
"""

mall_objs = Session().query(Mall).filter_by(active=True).all()
logger = get_logger("pdd")
counter = {}


async def sync_by_update_time_():
    global counter
    counter = {}
    session = Session()
    for mall_obj in mall_objs:
        try:
            logger.info("开始处理%s" % mall_obj.erp_name)
            counter[mall_obj.id] = {
                "created_count": 0,
                "updated_count": 0,
            }

            pdd = Pdd(mall_obj.client_id, mall_obj.client_secret, mall_obj.token)
            monitor_obj = session.query(Monitor).filter(
                Monitor.mall_id == mall_obj.id).order_by(-Monitor.last_run_ts).first()

            begin, end = get_time_range_30m(monitor_obj.last_run_ts)
            now = int(dt.now().timestamp()) - 30
            if now > end:
                last_ts = end
            else:
                last_ts = now
            result, err = await pdd.get_order_list_increment(
                start_updated_at=begin,
                end_updated_at=end,
                page=1,
                page_size=1,
            )
            err_handler("获取拼多多订单数据", err)

            total_count = result["total_count"]
            logger.info("%s -> %s 获取拼多多数据记录为%s" % (begin, end, total_count))

            if total_count > 0:
                total_page = int(total_count / 100) + 1
                tasks = []
                for page in range(1, total_page + 1):
                    result, err = await pdd.get_order_list_increment(
                        start_updated_at=begin,
                        end_updated_at=end,
                        page=page,
                        page_size=100,
                    )
                    err_handler("获取拼多多订单数据, 页%d" % page, err)
                    order_list = result["order_sn_list"]
                    tasks.append(to_db(session, pdd, order_list, mall_obj))

                await asyncio.gather(*tasks)
            session.add(get_monitor_obj(mall_obj, last_ts, total_count))
            logger.info(
                "%s 运行完毕, 新增%s, 修改%s"
                % (mall_obj.erp_name, counter[mall_obj.id]["created_count"], counter[mall_obj.id]["updated_count"])
            )
        except Exception as e:
            logger.error("定时任务-电商ERP订单-同步表单: %s \n %s" % (e, traceback.format_exc()))
        else:
            session.commit()
            await sync_privacy_info_()


async def sync_by_confirm_time_(days_before):
    global counter
    counter = {}
    session = Session()

    for d in range(days_before, -1, -1):
        day = dt.now() - timedelta(days=d)
        begin, end = get_time_range(day)
        for mall_obj in mall_objs:
            try:
                logger.info("开始处理%s" % mall_obj.erp_name)
                counter[mall_obj.id] = {
                    "created_count": 0,
                    "updated_count": 0,
                }

                pdd = Pdd(mall_obj.client_id, mall_obj.client_secret, mall_obj.token)

                result, err = await pdd.get_order_list(
                    start_confirm_at=begin,
                    end_confirm_at=end,
                    page=1,
                    page_size=1,
                )
                err_handler("获取拼多多订单数据", err)

                total_count = result["total_count"]
                logger.info("%s -> %s 获取拼多多数据记录为%s" % (begin, end, total_count))

                if total_count > 0:
                    total_page = int(total_count / 100) + 1
                    for page in range(total_page, 0, -1):
                        result, err = await pdd.get_order_list(
                            start_confirm_at=begin,
                            end_confirm_at=end,
                            page=page,
                            page_size=100,
                        )
                        err_handler("获取拼多多订单数据, 页%d" % page, err)
                        order_list = result["order_list"]
                        await to_db(session, pdd, order_list, mall_obj)

                session.add(get_monitor_obj(mall_obj, end, total_count))
                logger.info(
                    "%s 运行完毕, 新增%s, 修改%s"
                    % (mall_obj.erp_name, counter[mall_obj.id]["created_count"], counter[mall_obj.id]["updated_count"])
                )
            except Exception as e:
                logger.error("定时任务-电商ERP订单-同步表单: %s \n %s" % (e, traceback.format_exc()))
            else:
                session.commit()


async def to_db(session, pdd, order_list, mall_obj):
    for o in order_list:
        order_data = get_order_data(o)
        order_data["mall_id"] = mall_obj.id

        order_obj = session.query(Order).filter(Order.so_no == o["order_sn"]).first()
        if not order_obj:
            order_obj = Order(**order_data)
            session.add(order_obj)
            counter[mall_obj.id]["created_count"] += 1
        else:
            session.query(Item).filter(Item.order_id == order_obj.id).delete()
            for k, v in order_data.items():
                setattr(order_obj, k, v)
            counter[mall_obj.id]["updated_count"] += 1
        session.flush()

        if order_obj.after_sales_status == 10 and order_obj.after_sales_id is None:
            res, err = await pdd.get_refund_info(order_obj.so_no)
            err_handler("获取售后信息%s" % order_obj.so_no, err)
            order_obj.after_sales_id = res["id"]
            order_obj.after_sales_type = res["after_sales_type"]
            order_obj.goods_number = res["goods_number"]
            order_obj.refund_amount = res["refund_amount"] / 100

        item_count = 0
        for item in o["item_list"]:
            item_count += 1
            item_obj = Item(**{
                "order_id": order_obj.id,
                "qty": item["goods_count"],
                "goods_price": item["goods_price"],
                "goods_name": item["goods_name"],
                "goods_spec": item["goods_spec"],
                "goods_id": item["goods_id"],
                "sku_id": item["sku_id"],
                "outer_id": item["outer_id"],
            })
            session.add(item_obj)
        order_obj.item_count = item_count
        session.flush()


def get_order_data(json_obj):
    order_data = {
        "so_no": json_obj["order_sn"],
        "confirm_time": json_obj["confirm_time"],
        "so_created_at": json_obj["created_time"],
        "so_updated_at": json_obj["updated_at"],
        "confirm_status": json_obj["confirm_status"],
        "refund_status": json_obj["refund_status"],
        "after_sales_status": json_obj["after_sales_status"],
        "order_status": json_obj["order_status"],
        "risk_control_status": json_obj["risk_control_status"],
        "goods_amount": json_obj["goods_amount"],
        "discount_amount": json_obj["discount_amount"],
        "seller_discount": json_obj["seller_discount"],
        "platform_discount": json_obj["platform_discount"],
        "order_change_amount": json_obj["order_change_amount"],
        "capital_free_discount": json_obj["capital_free_discount"],
        "pay_amount": json_obj["pay_amount"],
        "postage": json_obj["postage"],
        "logistics_id": json_obj["logistics_id"],
        "tracking_number": json_obj["tracking_number"],
    }
    fee = 0.0
    if json_obj["service_fee_detail"]:
        for i in json_obj["service_fee"]:
            fee += i
    order_data["service_fee"] = fee

    if json_obj["shipping_time"]:
        order_data["shipping_time"] = json_obj["shipping_time"]

    if json_obj["province"]:
        order_data["buyer_account"] = json_obj["receiver_phone"]
        order_data["province"] = json_obj["province"]
        order_data["city"] = json_obj["city"]
        order_data["town"] = json_obj["town"]

    return order_data


def get_monitor_obj(mall_obj, last_run, total_count):
    return Monitor(**{
        "mall_id": mall_obj.id,
        "last_run_ts": last_run,
        "last_run_time": datetime.datetime.fromtimestamp(last_run),
        "total_count": total_count,
        "created_count": counter[mall_obj.id]["created_count"],
        "updated_count": counter[mall_obj.id]["updated_count"],
    })


def get_time_range(date: datetime.datetime):
    begin = dt.combine(date, datetime.time.min).timestamp()
    end = dt.combine(date, datetime.time.max).timestamp()
    now = dt.now().timestamp()
    if end > now:
        end = now
    return int(begin), int(end)


def get_time_range_30m(begin: int):
    begin = begin + 1
    end = begin + (30 * 60) - 1  # 29分59秒
    now = dt.now().timestamp() + (3 * 60)
    if end > now:
        end = now
    return int(begin), int(end)


async def sync_privacy_info_():
    # so_ids最多20条
    erp = Erp321()
    session = Session()
    while True:
        order_objs = session.query(Order).filter_by(buyer_account=None).limit(20).all()
        if len(order_objs) > 0:
            logger.info("db没有收货信息的条数%d" % len(order_objs))
        else:
            logger.info("没有需要从聚水潭获取数据的订单")
            break

        so_ids = [i.so_no for i in order_objs]
        try:
            result, err = await erp.get_orders(so_ids=so_ids, page_index=1, page_size=50)
            err_handler("获取订单数据%s" % so_ids, err)

            for db_obj in order_objs:
                for o_json in result["orders"]:
                    if db_obj.so_no == o_json["so_id"]:
                        if o_json["receiver_mobile"]:
                            db_obj.buyer_account = o_json["receiver_mobile"]
                            db_obj.province = o_json["receiver_state"]
                            db_obj.city = o_json["receiver_city"]
                            db_obj.town = o_json["receiver_district"]
                        else:
                            db_obj.buyer_account = ""
                            db_obj.province = ""
                            db_obj.city = ""
                            db_obj.town = ""
                        break
                else:
                    if not db_obj.buyer_account:
                        logger.warning("订单%s在聚水潭没有找到" % db_obj.so_no)
        except Exception as e:
            logger.error("订单号:%s补充收货人信息: %s \n %s" % (so_ids, e, traceback.format_exc()))
        else:
            session.commit()


def sync_privacy_info():
    cor = sync_privacy_info_()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(cor)
    if loop.is_running():
        loop.close()
    logger.info("fill_privacy_info()执行完成")


def sync_by_confirm_time(days_before):
    cor = sync_by_confirm_time_(days_before)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(cor)
    if loop.is_running():
        loop.close()
    logger.info("run_days()执行完成")


def sync_by_update_time():
    cor = sync_by_update_time_()
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(cor)
    if loop.is_running():
        loop.close()
    logger.info("main()执行完成")


if __name__ == "__main__":
    sync_by_update_time()
