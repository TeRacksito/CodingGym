from nextcord.ext.commands import Bot

class BotTypeError (TypeError):
    def __init__(self, obj, *args: object) -> None:
        message = f"Expected an object of type `{type(Bot)}`, but received: `{type(obj).__name__}`"
        super().__init__(message, *args)

class BranchWarning (Warning):
    def __init__(self, branch, *args: object) -> None:
        message = f"Expected to be on branch `main`, but received: `{branch}`"
        super().__init__(message, *args)
# class TableNameError (NameError):