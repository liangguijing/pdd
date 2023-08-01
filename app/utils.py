import asyncio


class ApiReturnError(Exception):
    pass


def err_handler(mark, error):
    if not error:
        return None
    raise ApiReturnError("[%s] %s" % (mark, error))


def set_value(data: dict):
    for k, v in data.items():
        data[k] = {"value": v}


def ez_run_async(coroutine):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(coroutine)
    loop.run_until_complete(asyncio.sleep(0.25))
    loop.close()
