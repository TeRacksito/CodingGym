import nextcord
import pygit2

def getCurrentBranch() -> str:
    repo = pygit2.Repository(".")
    return repo.head.shorthand

# async def message_deleted(channel: nextcord.TextChannel, message_id: int) -> bool:
#     # await nextcord.Message.channel.fetch_message(id= id)
#     channel.fetch_message(message_id)