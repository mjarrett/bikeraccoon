import bikeraccoon.bot as brbot

brbot.run(
    master_config_file='bot.credentials.json',
    bot_config_file='bot.avelo_quebec.json',
    path='./output',
    skip_zero_check=True,
)
