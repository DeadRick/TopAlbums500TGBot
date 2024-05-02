import json
import os
import traceback

from telegram import Update, KeyboardButton, ReplyKeyboardMarkup, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.constants import ParseMode
from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, CallbackQueryHandler, ContextTypes, \
    filters, CallbackContext

from albums import get_albums
from db import DbHandler
from logger import logger

GET_AN_ALBUM = "Прослушать альбом"
PROFILE = "Профиль"
SETTINGS = "Настройки"



async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    username = update.message.from_user.username
    first_name = update.message.from_user.first_name
    user_id = update.message.from_user.id
    DbHandler().add_user(user_id, username, first_name)
    print("Hi command")
    buttons = [[KeyboardButton(GET_AN_ALBUM), KeyboardButton(PROFILE)]]

    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="Добро пожаловать в бота, вдохновлённого знаменитым рейтингом журнала Rolling Stone \"The 500 Greatest Albums of All Time\"."
        ,reply_markup=ReplyKeyboardMarkup(buttons, resize_keyboard=True, ),
    )

async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(
        chat_id=update.effective_chat.id,
        text="/start - для начала работы с ботом \n\nВсе прослушанные альбомы отображаются в Профиле.\n\nЕсли у вас есть предложения или пожелания, пожалуйста, напишите: @DeadRick.",
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text: str = update.message.text

    # Free card processing
    if GET_AN_ALBUM in text:
        image, album_id, title, description = DbHandler().get_random_album(update.effective_message.from_user.id)
        album_text = f'{title} ({album_id})\n\n{description}\n\n'

        new_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("1", callback_data=f"1_{album_id}"),
             InlineKeyboardButton("2", callback_data=f"2_{album_id}"),
             InlineKeyboardButton("3", callback_data=f"3_{album_id}"),
             InlineKeyboardButton("4", callback_data=f"4_{album_id}"),
             InlineKeyboardButton("5", callback_data=f"5_{album_id}")],
            [InlineKeyboardButton("Пропустить", callback_data=f"0_{album_id}")],
        ])

        if len(album_text) > 4000:
            album_text = album_text[:4000] + "Оцените альбом после прослушивания от 1 до 5 или пропустите его:"
            await update.message.reply_photo(photo=image, caption=album_text, reply_markup=new_markup, parse_mode=ParseMode.HTML)

        else:
            album_text = album_text + "Оцените альбом после прослушивания от 1 до 5 или пропустите его:"
            await update.message.reply_photo(photo=image, caption=album_text, reply_markup=new_markup,
                                             parse_mode=ParseMode.HTML)

    if PROFILE in text:
        message = DbHandler().get_all_albums(update.effective_message.from_user.id)
        if len(message) > 4096:
            for x in range(0, len(message), 4096):
                await update.message.reply_text(text=message[x:x + 4096], parse_mode=ParseMode.HTML)
        else:
            await update.message.reply_text(text=message, parse_mode=ParseMode.HTML)
    if "GO" in text:
        data = text.split()
        links = [data[1]]
        # Scrap albums from Rolling Stones
        get_albums(links, int(data[2]))
        await  update.message.reply_text("done")

async def error_hand(update: Update, context: ContextTypes.DEFAULT_TYPE):
    error_traceback = ''.join(traceback.format_exception(None, context.error, context.error.__traceback__))
    error_message = f"Произошла ошибка:\n\n{error_traceback}"
    logger.error(f'Update {update} caused error {error_message}')


async def query_handler(update: Update, context: CallbackContext):
    query = update.callback_query
    data = query.data
    data_info = data.split('_')

    if data == "no_action":
        return

    if data_info[0] == 'change':
        new_markup = InlineKeyboardMarkup([
            [InlineKeyboardButton("1", callback_data=f'1_{data_info[2]}'),
             InlineKeyboardButton("2", callback_data=f'2_{data_info[2]}'),
             InlineKeyboardButton("3", callback_data=f'3_{data_info[2]}'),
             InlineKeyboardButton("4", callback_data=f'4_{data_info[2]}'),
             InlineKeyboardButton("5", callback_data=f'5_{data_info[2]}')],
            [InlineKeyboardButton("Пропустить", callback_data=f'0_{data_info[2]}')],
        ])
        await update.effective_message.edit_reply_markup(reply_markup=new_markup)
        await  query.answer()
        return

    print(data)
    text = f"{data_info[0]}/5 {'⭐️' * int(data_info[0])}"
    if int(data_info[0]) == 0:
        text = "Альбом пропущен"

    DbHandler().update_rate(update.effective_user.id, album_id=(data_info[1]), rate=int(data_info[0]))
    new_keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(text, callback_data="no_action")], [InlineKeyboardButton("Изменить оценку", callback_data=f"change_{data}")]])
    await update.effective_message.edit_reply_markup(reply_markup=new_keyboard)
    await  query.answer()

class TgHandler:
    def __init__(self):
        BOT_TOKEN = os.getenv(key="BOT_TOKEN", default="7086079456:AAHI7NWCKBedxLT9-cnsoA0IVqB2oaK-VL0")
        self.application = ApplicationBuilder().token(BOT_TOKEN).build()
        self.add_user_handlers()
    def add_user_handlers(self):
        # /start and /help handler
        self.application.add_handler(CommandHandler("start", start_command))
        self.application.add_handler(CommandHandler("help", help_command))
        self.application.add_error_handler(error_hand)
        self.application.add_handler(CallbackQueryHandler(query_handler))


        # Message handler
        self.application.add_handler(MessageHandler(filters.TEXT, handle_message))

    def local_run(self):
        print("Starting bot...")

        self.application.run_polling()

    async def cloud_run(self, event):
        try:
            logger.info('Processing update...')
            await self.application.initialize()
            for message in event["messages"]:
                await self.application.process_update(
                    Update.de_json(json.loads(message["details"]["message"]["body"]), self.application.bot)
                )
                logger.info(f'Processed update {message["details"]["message"]["body"]}')
                return 'Success'
        except Exception as e:
            logger.info(f"Failed to process update with {e}")
        return 'Failure'
