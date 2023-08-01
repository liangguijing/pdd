import asyncio
import aiohttp
from datetime import datetime
from hashlib import md5
from typing import Union


class Pdd:
    _base_url = "https://gw-api.pinduoduo.com/api/router"
    _headers = {"content-type": "application/json"}
    _logistics_company = {}  # "id": 463,

    def __init__(self, client_id, client_secret, access_token):
        self._client_id = client_id
        self._client_secret = client_secret
        self._access_token = access_token
        self._mall_info = {}

    def get_common_params(self, api_name):
        return {  # 必填的公共请求参数
            "access_token": self._access_token,
            "client_id": self._client_id,  # POP分配给应用的client_id
            "timestamp": get_current_timestamp(),  # UNIX时间戳，单位秒，需要与拼多多服务器时间差值在10分钟内
            "type": api_name,  # API接口名称
        }

    @property
    async def mall_info(self):
        if not self._mall_info:
            self._mall_info = await self._get_mall_info()
        return self._mall_info

    async def send_post(self, data: dict):
        data["sign"] = get_sign(data)
        return await send_pdd_request("POST", self._base_url, json=data, headers=self._headers)

    async def get_access_token(self, code):
        api_name = "pdd.pop.auth.token.create"
        data = self.get_common_params(api_name)
        data["code"] = code
        result, err = await self.send_post(data)
        if not err:
            print(result)
            return result["pop_auth_token_create_response"]["access_token"]
        raise Exception("获取拼多多access_token失败 %s" % err)

    async def get_order_list(self,
                             start_confirm_at: int,
                             end_confirm_at: int,
                             *,
                             is_basic=False,
                             order_status=5,
                             page=1,
                             page_size=100,
                             refund_status=5,
                             trade_type: Union[None, int] = None,
                             use_has_next: Union[None, bool] = None,
                             ):
        """
        pdd.order.list.get          根据成交时间查询订单列表（只能获取到成交时间三个月以内的交易信息）
        pdd.order.basic.list.get    根据成团时间查询订单列表，只有订单基础信息，不包含消费者信息

        :param start_confirm_at:    必填，成交时间开始时间的时间戳
        :param end_confirm_at:      必填，成交时间结束时间的时间戳
        :param is_basic:            是否只查基础数据
        :param order_status:        发货状态，1：待发货，2：已发货待签收，3：已签收 5：全部
        :param page:                返回页码 默认 1，页码从 1 开始 PS：当前采用分页返回，数量和页数会一起传，如果不传，则采用 默认值
        :param page_size:           返回数量，默认 100。最大 100
        :param refund_status:       售后状态 1：无售后或售后关闭，2：售后处理中，3：退款中，4： 退款成功 5：全部
        :param trade_type:          订单类型 0-普通订单 ，1- 定金订单
        :param use_has_next:        是否启用has_next的分页方式，如果指定true,则返回的结果中不包含总记录数，但是会新增一个是否存在
                                        下一页的的字段，通过此种方式获取增量交易，效率在原有的基础上有80%的提升。
        :return:
        """
        if not is_basic:
            api_name = "pdd.order.list.get"
        else:
            api_name = "pdd.order.basic.list.get"
        data = self.get_common_params(api_name)
        data["start_confirm_at"] = start_confirm_at
        data["end_confirm_at"] = end_confirm_at
        data["order_status"] = order_status
        data["page"] = page
        data["page_size"] = page_size
        data["refund_status"] = refund_status
        if trade_type:
            data["trade_type"] = trade_type
        if use_has_next:
            data["use_has_next"] = "true"

        result, err = await self.send_post(data)
        if not err:
            if not is_basic:
                result = result["order_list_get_response"]
            else:
                result = result["order_basic_list_get_response"]
        return result, err

    async def get_logistics_company(self, _id):
        if not _id:
            return ""
        if not self._logistics_company:
            api_name = "pdd.logistics.companies.get"
            data = self.get_common_params(api_name)
            result, err = await self.send_post(data)
            if not err:
                result = result["logistics_companies_get_response"]["logistics_companies"]
                for i in result:
                    self._logistics_company[i["id"]] = i["logistics_company"]
        return self._logistics_company[_id]

    async def get_order_status(self, order_sns):
        """
        pdd.order.status.get 获取订单的状态
        :param order_sns: 20150909-452750051,20150909-452750134 用逗号分开
        :return: []
        """
        api_name = "pdd.order.status.get"
        data = self.get_common_params(api_name)
        data["order_sns"] = order_sns
        result, err = await self.send_post(data)
        if not err:
            result = result["order_status_get_response"]["order_status_list"]
        return result, err

    async def _get_mall_info(self):
        api_name = "pdd.mall.info.get"
        data = self.get_common_params(api_name)
        result, err = await self.send_post(data)
        if not err:
            return result["mall_info_get_response"]
        raise Exception("Get mall info failed!")

    async def get_logistics_address(self):
        # 获取拼多多标准地址库
        api_name = "pdd.logistics.address.get"
        data = self.get_common_params(api_name)
        result, err = await self.send_post(data)
        if not err:
            result = result["logistics_address_get_response"]["logistics_address_list"]
        return result, err

    async def get_order_info(self, order_sn):
        # 查询单个订单详情（只能获取到成交时间三个月以内的交易信息）
        api_name = "pdd.order.information.get"
        data = self.get_common_params(api_name)
        data["order_sn"] = order_sn
        result, err = await self.send_post(data)
        if not err:
            result = result["order_info_get_response"]
        return result, err

    async def get_order_list_increment(self,
                                       start_updated_at: int,
                                       end_updated_at: int,
                                       *,
                                       is_lucky_flag=0,
                                       order_status=5,
                                       page=1,
                                       page_size=100,
                                       refund_status=5,
                                       trade_type: [None, int] = None,
                                       use_has_next: [None, bool] = None,
                                       ):
        """
        查询订单增量，注：虚拟订单充值手机号信息无法通过此接口获取，请联系虚拟类目运营人员。 拉取卖家已卖出的增量交易数据（只能获取到成交时间三个月以内的交易信息）
        ①. 一次请求只能查询时间跨度为30分钟的增量交易记录，即end_updated_at - start_updated_at<= 30min。
        ②. 通过从后往前翻页的方式以及结束时间不小于拼多多系统时间前3min可以避免漏单问题。
        :param start_updated_at:    必填，最后更新时间开始时间的时间戳
        :param end_updated_at:      必填，最后更新时间结束时间的时间戳
        :param is_lucky_flag:       订单类型（是否抽奖订单），0-全部，1-非抽奖订单，2-抽奖订单
        :param order_status:        发货状态，1-待发货，2-已发货待签收，3-已签收，5-全部
        :param page:                返回页码，默认 1，页码从 1 开始 PS：当前采用分页返回，数量和页数会一起传，如果不传，则采用 默认值；
                                    注：必须采用倒序的分页方式（从最后一页往回取）才能避免漏单问题。
        :param page_size:           返回数量，默认 100。最大 100
        :param refund_status:       售后状态，1-无售后或售后关闭，2-售后处理中，3-退款中，4-退款成功 5-全部
        :param trade_type:          订单类型： 0-普通订单、1-定金订单 不传为全部
        :param use_has_next:        是否启用has_next的分页方式，如果指定true,则返回的结果中不包含总记录数，但是会新增一个是否存在下一页的的字段，
                                    通过此种方式获取增量交易，效率在原有的基础上有80%的提升。
        :return:
        """
        api_name = "pdd.order.number.list.increment.get"
        data = self.get_common_params(api_name)
        data["start_updated_at"] = start_updated_at
        data["end_updated_at"] = end_updated_at
        data["is_lucky_flag"] = is_lucky_flag
        data["order_status"] = order_status
        data["page"] = page
        data["page_size"] = page_size
        data["refund_status"] = refund_status
        if trade_type:
            data["trade_type"] = trade_type
        if use_has_next:
            data["use_has_next"] = "true"
        result, err = await self.send_post(data)
        if not err:
            result = result["order_sn_increment_get_response"]
        return result, err

    async def get_refund_info(self, order_sn, after_sales_id: [None, int] = None):
        """
        查询单个售后单详情
        :param order_sn:        订单号
        :param after_sales_id:  售后单id
        :return:
        """
        api_name = "pdd.refund.information.get"
        data = self.get_common_params(api_name)
        data["order_sn"] = order_sn
        if after_sales_id:
            data["after_sales_id"] = after_sales_id
        return await self.send_post(data)

    async def get_refund_list_increment(self,
                                        start_updated_at: int,
                                        end_updated_at: int,
                                        *,
                                        order_sn="",
                                        after_sales_status=10,  # 退款成功
                                        after_sales_type=1,  # 全部
                                        page=1,
                                        page_size=100,
                                        ):
        """
        售后列表增量查询, 开始时间结束时间间距不超过 30 分钟
        :param start_updated_at:
        :param end_updated_at:
        :param order_sn:
        :param after_sales_status:
        :param after_sales_type:
        :param page:
        :param page_size:
        :return:
        """
        api_name = "pdd.refund.list.increment.get"
        data = self.get_common_params(api_name)
        data["start_updated_at"] = start_updated_at
        data["end_updated_at"] = end_updated_at
        if order_sn:
            data["order_sn"] = order_sn
        data["after_sales_status"] = after_sales_status
        data["after_sales_type"] = after_sales_type
        data["page"] = page
        data["page_size"] = page_size
        result, err = await self.send_post(data)
        if not err:
            result = result["refund_increment_get_response"]
        return result, err


def get_sign(params: dict):
    sorted_dict = sorted(params.items())
    text = "".join([''.join((str(k), str(v))) for k, v in sorted_dict])
    text = "{client_secret}{text}{client_secret}".format(client_secret=CLIENT_SECRET, text=text)
    return md5(text.encode("utf-8")).hexdigest().upper()


def get_current_timestamp():
    return int(datetime.now().timestamp())


async def send_pdd_request(method, url, headers=None, data=None, params=None, json=None):
    result = err = None
    try:
        async with aiohttp.ClientSession() as session:
            async with session.request(method, url, params=params, data=data, headers=headers, json=json) as response:
                result = await response.json(encoding="utf-8")
    except Exception as e:
        err = str(e)
        return result, err

    if result and result.get("error_response"):
        err = result["error_response"]
        error_code = result["error_response"]["error_code"]
        if error_code in {52101, 52102, 52103, 70031}:  # 当前接口被限流/接口暂时不可用/服务暂时不可用/调用过于频繁
            print("拼多多接口限流, 稍后重试... %s" % err)
            await asyncio.sleep(0.2)
            return await send_pdd_request(method, url, headers=headers, data=data, params=params, json=json)
        result = None
    return result, err
