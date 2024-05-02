import asyncio

from tgbot import TgHandler

#tgbot = TgHandler().local_run()
def handler(event: dict, context: str):
    print("Im here!")
    result = asyncio.get_event_loop().run_until_complete(TgHandler().cloud_run(event))
    return {
        'statusCode': 200,
        'body': result
    }
