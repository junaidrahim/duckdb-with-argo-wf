import ibis


def lineitem_query(lineitem: ibis.Table) -> ibis.Table:
    q_final = (
        lineitem.group_by(["l_returnflag", "l_linestatus"])
        .aggregate(
            sum_qty=lineitem["l_quantity"].sum(),
            sum_base_price=lineitem["l_extendedprice"].sum(),
            sum_disc_price=(
                lineitem["l_extendedprice"] * (1 - lineitem["l_discount"])
            ).sum(),
            sum_charge=(
                lineitem["l_extendedprice"]
                * (1 - lineitem["l_discount"])
                * (1 + lineitem["l_tax"])
            ).sum(),
            count_order=lambda lineitem: lineitem.count(),
        )
        .order_by(["l_returnflag", "l_linestatus"])
    )

    return q_final


def orders_query(orders: ibis.Table) -> ibis.Table:
    q_final = (
        orders.group_by(["o_orderpriority", "o_orderstatus"])
        .aggregate(
            order_volume=orders["o_totalprice"].sum(),
            count_order=lambda orders: orders.count(),
        )
        .order_by(["o_orderpriority", "o_orderstatus"])
    )

    print(ibis.to_sql(q_final))

    return q_final
