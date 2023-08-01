import uuid

from sqlalchemy import Column, Boolean, Integer, DateTime, ForeignKey, String, Text, Numeric, func, \
    MetaData, BigInteger, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship, sessionmaker, scoped_session, declarative_base
from sqlalchemy.pool import QueuePool


engine = create_engine("postgresql+psycopg2://jack:jack_2023@127.0.0.1:5432/order_db_pdd", poolclass=QueuePool)
meta_data = MetaData()
Base = declarative_base(metadata=meta_data)
Session = scoped_session(sessionmaker(engine))


class Mall(Base):
    __tablename__ = "mall"

    id = Column(Integer, primary_key=True)
    order = relationship("Order", back_populates="mall")

    org_id = Column(Integer, nullable=True)  # pdd mall_id
    name = Column(Text)  # pdd mall_name
    erp_id = Column(Integer)  # 聚水潭店铺编号
    erp_name = Column(String)  # 聚水潭店铺名称
    platform = Column(String)  # 所属平台
    client_id = Column(String)
    client_secret = Column(String)
    token = Column(String)  # access token
    active = Column(Boolean, default=True)
    expires_at = Column(DateTime)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class Order(Base):
    __tablename__ = "order"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    mall_id = Column(Integer, ForeignKey("mall.id"))
    mall = relationship("Mall", back_populates="order")
    item = relationship("Item", back_populates="order")

    sync = Column(Integer, default=0)
    so_no = Column(String, index=True)
    confirm_time = Column(DateTime)  # 订单成交时间/付款时间
    so_created_at = Column(DateTime)  # 订单创建时间
    so_updated_at = Column(DateTime)  # 订单更新时间
    confirm_status = Column(Integer)  # 成交状态：0：未成交、1：已成交、2：已取消
    refund_status = Column(Integer)  # 退款状态，枚举值：1：无售后或售后关闭，2：售后处理中，3：退款中，4： 退款成功
    after_sales_status = Column(Integer)  # 售后状态
    order_status = Column(Integer)  # 发货状态
    risk_control_status = Column(Integer)  # 订单审核状态（0-正常订单， 1-审核中订单）
    buyer_account = Column(String, nullable=True)  # 存收货人手机号
    province = Column(String, nullable=True)
    city = Column(String, nullable=True)
    town = Column(String, nullable=True)
    goods_amount = Column(Numeric(10, 2))  # 原价
    discount_amount = Column(Numeric(10, 2))  # 折扣金额（元），折扣金额=平台优惠+商家优惠+团长免单优惠金额
    seller_discount = Column(Numeric(10, 2))  # 店铺优惠金额
    platform_discount = Column(Numeric(10, 2))  # 平台优惠
    order_change_amount = Column(Numeric(10, 2))  # 订单改价
    capital_free_discount = Column(Numeric(10, 2))  # 团长免单优惠金额
    service_fee = Column(Numeric(10, 2))  # 服务费明细列表
    pay_amount = Column(Numeric(10, 2))  # 支付金额（元），支付金额=商品金额-折扣金额+邮费+服务费
    postage = Column(Numeric(10, 2))
    shipping_time = Column(DateTime, nullable=True)  # 发货日期
    logistics_id = Column(Integer)
    tracking_number = Column(String, nullable=True)
    item_count = Column(Integer)
    after_sales_id = Column(BigInteger, nullable=True)  # 售后单id
    after_sales_type = Column(Integer, nullable=True)  # 售后类型 1-仅退款，2-退货退款，3-换货，4-补寄，5-维修
    goods_number = Column(Integer, nullable=True)  # 商品数量
    refund_amount = Column(Numeric(10, 2), nullable=True)   # 退款金额
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class Item(Base):
    __tablename__ = "item"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    order_id = Column(UUID(as_uuid=True), ForeignKey("order.id"))
    order = relationship("Order", back_populates="item")

    qty = Column(Integer)
    goods_price = Column(Numeric(10, 2))
    goods_name = Column(String)
    goods_spec = Column(String)
    goods_id = Column(String)
    sku_id = Column(String)
    outer_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=func.now(), onupdate=func.now())


class Monitor(Base):
    __tablename__ = "monitor"

    id = Column(Integer, primary_key=True)
    mall_id = Column(Integer, ForeignKey("mall.id"))
    last_run_ts = Column(Integer)
    last_run_time = Column(DateTime)
    total_count = Column(Integer, default=0)
    created_count = Column(Integer, default=0)
    updated_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now(), onupdate=func.now())


if __name__ == "__main__":
    Base.metadata.create_all(engine)
