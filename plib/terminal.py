"""
Terminal Handler Module created by TeRacksito

This module handles the stream, errors and logging system.

Raises
------
`ValueError`
    If the logging level of error handler is set at DEBUG or any lower.
`ValueError`
    If the logging level of error handler is set by using an undefined name.
`IOError`
    If the module is unable to create a unique logging file.
"""
# pylint: disable=too-many-lines

import datetime
import io
import logging
import logging.config
import os
import re
import sys
import traceback
import codecs
from encodings.cp1252 import encoding_table

class SGR:
    """
    SGR class

    An implementation of Select Graphic Rendition.

    You can use this class to color a string. There are several ways of using
    this class.

    Manually
    --------

    You can use SGR manually, but it is recommended to use it with the `format()`,
    `apply()` and `set()` functions. 
    
    - To use SGR manually, you must first call the `EXT` method,
    this will apply the starter of a SGR code (`"\x1b["`) to your string. Then you
    must call the desired style, `Foreground.black` for example, note that you
    can call as many styles as you want here, but they must be separated by ";". 
    When you are done adding styles, you must finalize the SGR code calling the
    `F` method, this will apply the ending of a SGR code (`"m"`) to your string.
    Finally, you should add the content to be stylized. Now, normally, you should
    call the `default_format` method to avoid styling the following strings.
    Note that, in the terminal, without setting a default format at the end,
    you will stylize everything that comes after until the style is overwritten.

    Example of this explanation:

    `f"{SGR.EXT}{SGR.Foreground.black}{SGR.F}This string will be black!{SGR.default_format}"`

    The string will be:

    `"\x1b[30mThis string will be black!\x1b[0m"`

    Automatically
    -------------

    This is the recommended way to use this implementation of SGR.

    - The main function is `SGR.format()`. Just pass as parameters the
    content to be stylized and then the styles you want, as many as you want.
    This will automatically set the `EXT`, the styles separated by ";", the `F` and,
    very important to know, the `default_format` at the end of your content.

    Example using `SGR.format()`:

    `"SGR.format("This string will be black!, SGR.Foreground.black)"`

    The string will be:

    `"\x1b[30mThis string will be black!\x1b[0m"`

    Other
    -----

    There are also `apply()` and `set()` function.

    - `apply()` is used to apply a style to a string without changing the
    followings and keep the style before it. But, you need to know this.
    The downsides of this function are that only works with editable styling,
    that means only `bold`, `underline` and `negative`. Also, if any of this editable
    styles are already activate and you add it to the apply function, `apply()` will deactivate
    them at the end, so is not recommended to apply a editable style if it is already applied.
    
    - `set()` is used exactly as `format()`, the difference is that `set()` will not call
    `default_format` at the end and it do not need the content to be stylized. 
    This is useful if you want to have more control but without using SGR manually.
    You should add the content desired to be stylized after `set()` function, you could
    concatenate them using `f-string`, for example.  
    """

    class ColorChem:
        """
        ColorChem class

        An internal class that should not be call outside.
        """

        @classmethod
        @property
        def black(cls) -> str:
            """
            @classmethod

            black property function

            Returns `black` in SGR code for the specific context.
            Context could be `Foreground`, `Background` and their `Bright`
            versions.

            Returns
            -------
            `str`
                Partial/incomplete SGR code string.
            """
            # pylint: disable=no-member
            return f"{cls.mode}0"

        @classmethod
        @property
        def red(cls) -> str:
            """
            @classmethod

            red property function

            Returns `red` in SGR code for the specific context.
            Context could be `Foreground`, `Background` and their `Bright`
            versions.

            Returns
            -------
            `str`
                Partial/incomplete SGR code string.
            """
            # pylint: disable=no-member
            return f"{cls.mode}1"

        @classmethod
        @property
        def green(cls) -> str:
            """
            @classmethod

            green property function

            Returns `green` in SGR code for the specific context.
            Context could be `Foreground`, `Background` and their `Bright`
            versions.

            Returns
            -------
            `str`
                Partial/incomplete SGR code string.
            """
            # pylint: disable=no-member
            return f"{cls.mode}2"

        @classmethod
        @property
        def yellow(cls) -> str:
            """
            @classmethod

            yellow property function

            Returns `yellow` in SGR code for the specific context.
            Context could be `Foreground`, `Background` and their `Bright`
            versions.

            Returns
            -------
            `str`
                Partial/incomplete SGR code string.
            """
            # pylint: disable=no-member
            return f"{cls.mode}3"

        @classmethod
        @property
        def blue(cls) -> str:
            """
            @classmethod

            blue property function

            Returns `blue` in SGR code for the specific context.
            Context could be `Foreground`, `Background` and their `Bright`
            versions.

            Returns
            -------
            `str`
                Partial/incomplete SGR code string.
            """
            # pylint: disable=no-member
            return f"{cls.mode}4"

        @classmethod
        @property
        def magenta(cls) -> str:
            """
            @classmethod

            magenta property function

            Returns `magenta` in SGR code for the specific context.
            Context could be `Foreground`, `Background` and their `Bright`
            versions.

            Returns
            -------
            `str`
                Partial/incomplete SGR code string.
            """
            # pylint: disable=no-member
            return f"{cls.mode}5"

        @classmethod
        @property
        def cyan(cls) -> str:
            """
            @classmethod

            cyan property function

            Returns `cyan` in SGR code for the specific context.
            Context could be `Foreground`, `Background` and their `Bright`
            versions.

            Returns
            -------
            `str`
                Partial/incomplete SGR code string.
            """
            # pylint: disable=no-member
            return f"{cls.mode}6"

        @classmethod
        @property
        def white(cls) -> str:
            """
            @classmethod

            white property function

            Returns `white` in SGR code for the specific context.
            Context could be `Foreground`, `Background` and their `Bright`
            versions.

            Returns
            -------
            `str`
                Partial/incomplete SGR code string.
            """
            # pylint: disable=no-member
            return f"{cls.mode}7"

        @classmethod
        def rgb(cls, red: int, green: int, blue: int) -> str:
            """
            @classmethod

            rgb function

            Used to set a custom RGB SGR code color for the specific
            context. Context could be `Foreground`, `Background` and
            their `Bright` versions.

            This is not a property, so is used like this:

            `f"SGR.format("Test", SGR.Foreground.rgb(80, 20, 120))"`

            Parameters
            ----------
            red : `int`
                Red integer value for the RGB color format, 0 - 255.
            green : `int`
                Green integer value for the RGB color format, 0 - 255.
            blue : `int`
                Blue integer value for the RGB color format, 0 - 255.

            Returns
            -------
            `str`
                Partial/incomplete SGR code string.
            """
            # pylint: disable=no-member
            return f"{cls.mode}8;2;{str(red)};{str(green)};{str(blue)}"

        @classmethod
        @property
        def default(cls) -> str:
            """
            @classmethod

            default property function

            Sets `default` in SGR code for the specific context.
            Context could be `Foreground`, `Background` and their `Bright`
            versions. This is useful if you want to set Foreground to default
            style, but without changing Background style.

            Returns
            -------
            `str`
                Partial/incomplete SGR code string.
            """
            # pylint: disable=no-member
            return f"{cls.mode}9"

    class Foreground(ColorChem):
        """
        Foreground class

        Used to set the Foreground color of the string.
        Does nothing itself.

        Available properties of `Foreground`:

        - `black` -> `SGR.Foreground.black`

        - `red` -> `SGR.Foreground.red`

        - `green` -> `SGR.Foreground.green`

        - `yellow` -> `SGR.Foreground.yellow`

        - `blue` -> `SGR.Foreground.blue`

        - `magenta` -> `SGR.Foreground.magenta`

        - `cyan` -> `SGR.Foreground.cyan`

        - `white` -> `SGR.Foreground.white`

        - `default` -> `SGR.Foreground.default`

        You can also use the function `rgb()` to set a
        custom color.

        - `rgb` -> `SGR.Foreground.rgb(red_int, green_int, blue_int)`
        """
        mode = "3"

    class Background(ColorChem):
        """
        Background class

        Used to set the Background color of the string.
        Does nothing itself.

        Available properties of `Background`:

        - `black` -> `SGR.Background.black`

        - `red` -> `SGR.Background.red`

        - `green` -> `SGR.Background.green`

        - `yellow` -> `SGR.Background.yellow`

        - `blue` -> `SGR.Background.blue`

        - `magenta` -> `SGR.Background.magenta`

        - `cyan` -> `SGR.Background.cyan`

        - `white` -> `SGR.Background.white`

        - `default` -> `SGR.Background.default`

        You can also use the function `rgb()` to set a
        custom color.

        - `rgb` -> `SGR.Background.rgb(red_int, green_int, blue_int)`
        """
        mode = "4"


    class BrightForeground(ColorChem):
        """
        BrightForeground class

        Used to set the BrightForeground color of the string.
        Does nothing itself.

        Available properties of `BrightForeground`:

        - `black` -> `SGR.BrightForeground.black`

        - `red` -> `SGR.BrightForeground.red`

        - `green` -> `SGR.BrightForeground.green`

        - `yellow` -> `SGR.BrightForeground.yellow`

        - `blue` -> `SGR.BrightForeground.blue`

        - `magenta` -> `SGR.BrightForeground.magenta`

        - `cyan` -> `SGR.BrightForeground.cyan`

        - `white` -> `SGR.BrightForeground.white`
        """
        mode = "9"

    class BrightBackground(ColorChem):
        """
        BrightBackground class

        Used to set the BrightBackground color of the string.
        Does nothing itself.

        Available properties of `BrightBackground`:

        - `black` -> `SGR.BrightBackground.black`

        - `red` -> `SGR.BrightBackground.red`

        - `green` -> `SGR.BrightBackground.green`

        - `yellow` -> `SGR.BrightBackground.yellow`

        - `blue` -> `SGR.BrightBackground.blue`

        - `magenta` -> `SGR.BrightBackground.magenta`

        - `cyan` -> `SGR.BrightBackground.cyan`

        - `white` -> `SGR.BrightBackground.white`
        """
        mode = "10"

    @classmethod
    @property
    def EXT(cls) -> str:
        """
        @classmethod

        EXT property function

        This is the starter of a SGR code. Always at the start
        of the SGR code.

        Returns
        -------
        `str`
            Partial/incomplete SGR code string.
        """
        # pylint: disable=invalid-name
        return "\x1b["

    @classmethod
    @property
    def F(cls) -> str:
        """
        @classmethod

        F property function

        This is the finisher of a SGR code. Always at the end
        of the SGR code. It indicates that the SGR code will be
        a text formatting one.

        Returns
        -------
        `str`
            Partial/incomplete SGR code string.
        """
        # pylint: disable=invalid-name
        return "m"

    @classmethod
    @property
    def default(cls) -> str:
        """
        @classmethod

        default property function

        Normally `default` will never be used, since it just
        returns `"0"`, as 0 is the constructor default formatting
        method. Is recommended to use `default_format` instead.

        Returns
        -------
        'str'
            Partial/incomplete SGR code string.
        """
        return "0"

    @classmethod
    @property
    def bold(cls) -> str:
        """
        @classmethod

        bold property function

        Set the bold style to the string. It can be removed with
        `remove_bold` property function.

        Returns
        -------
        `str`
            Partial/incomplete SGR code string.
        """
        return "1"

    @classmethod
    @property
    def remove_bold(cls) -> str:
        """
        @classmethod

        remove_bold property function

        It removes the bold style set by the `bold` property
        function.

        Returns
        -------
        `str`
            Partial/incomplete SGR code string.
        """
        return "22"

    @classmethod
    @property
    def underline(cls) -> str:
        """
        @classmethod

        underline property function

        Set the underline style to the string. It can be removed
        with `remove_underline` property function.

        Returns
        -------
        `str`
            Partial/incomplete SGR code string.
        """
        return "4"

    @classmethod
    @property
    def remove_underline(cls) -> str:
        """
        @classmethod

        remove_underline property function

        It removes the underline style set by the `underline`
        property function.

        Returns
        -------
        `str`
            Partial/incomplete SGR code string.
        """
        return "24"

    @classmethod
    @property
    def negative(cls) -> str:
        """
        @classmethod

        negative property function

        Set the negative style to the string. It can be removed
        with `remove_negative` property function.

        Returns
        -------
        `str`
            Partial/incomplete SGR code string.
        """
        return "7"

    @classmethod
    @property
    def remove_negative(cls) -> str:
        """
        @classmethod

        remove_negative property function

        It removes the underline style set by the `negative`
        property function.

        Returns
        -------
        `str`
            Partial/incomplete SGR code string.
        """
        return "27"

    @classmethod
    @property
    def default_format(cls) -> str:
        """
        @classmethod

        default_format property function

        Removes all styles set before. It includes automatically
        the `EXT` and `F` methods. `default_format` is already
        included at the end of the `SGR.format()` function.

        Returns
        -------
        `str`
            Complete SGR code string.
        """
        return f"{cls.EXT}0{cls.F}"

    @classmethod
    def format(cls, content: str, *values: str) -> str:
        """
        @classmethod

        format function

        The most automatized way to use SGR. It includes the
        `EXT`, `F` and `default_format` methods in the
        corresponding places. 

        Parameters
        ----------
        content : `str`
            The content as a string to be stylized.
        
        *values : `str`
            The SGR styles to be applied, as many as wanted.

        Returns
        -------
        `str`
            Complete SGR code string with `default_format`
            at the end.
        """
        values_str = ';'.join(str(v) for v in values)
        composition = f"{cls.EXT}{values_str}{cls.F}{content}{cls.default_format}"
        return composition

    @classmethod
    def set(cls, *values: str) -> str:
        """
        @classmethod

        set function

        Similar to `format()` function. It just sets the style
        SGR code with `EXT` and `F` methods, but does not require
        the content to be stylized itself and does not use
        `default_format` at the end. This means that the style
        is applied for everything following `set()` function until
        `default_format` is called manually or by other function
        like `format()`.

        Parameters
        ----------        
        *values : `str`
            The SGR styles to be applied, as many as wanted.

        Returns
        -------
        `str`
            Complete SGR code string.
        """
        values_str = ';'.join(str(v) for v in values)
        composition = f"{cls.EXT}{values_str}{cls.F}"
        return composition

    @classmethod
    def apply(cls, content: str, *values: str) -> str:
        """
        @classmethod

        apply function

        Applies an editable style such as `bold`, `underline`
        and `negative` to the content and removes the
        editable style for the following strings. Useful to
        apply, for example, a `bold` style to a name inside a
        `format()` function.

        Parameters
        ----------
        content : `str`
            The content as a string to be stylized.
        
        *values : `str`
            The SGR editable styles to be applied, as many
            as wanted. Only works with `bold`, `underline`
            and `negative` editable styles.
        
        Returns
        -------
        `str`
            Complete SGR code string.
        """

        values_str = ';'.join(str(v) for v in values)
        remove_values_str = ';'.join(f"2{str(v)}" for v in values)

        composition = f"{cls.EXT}{values_str}{cls.F}{content}{cls.EXT}{remove_values_str}{cls.F}"
        return composition

def remove_sgr(string: str) -> str:
    """
    remove_sgr function

    Attempts to remove all SGR codes from a given string.

    Parameters
    ----------
    string : `str`
        The string where SGR codes will be attempted to be removed.
        If the string does not have any SGR code, then no changes
        should be applied and the same string should be returned.

    Returns
    -------
    `str`
        A string that should be without any SGR code.
    """

    # regex pattern that should be able to remove SGR codes.
    sgr_pattern = r"\x1b\[[0-9;]*[a-zA-Z]"
    return re.sub(sgr_pattern, "", string)

class Timestamp:
    """
    Timestamp Class

    A class to easily create timestamps.
    """
    def __init__(self, timestamp: datetime.datetime = None):
        if timestamp is None:
            self.timestamp = datetime.datetime.now()
        else:
            self.timestamp = timestamp

    @property
    def now(self) -> datetime.datetime:
        """
        now function

        Updates the timestamp of the class.

        Returns
        -------
        `Timestamp`
            Updated version of the class.
        """
        timestamp = datetime.datetime.now()
        return self.__class__(timestamp)

    @property
    def terminal(self) -> str:
        """
        terminal function

        Returns the timestamp formatted for terminal purposes.

        Returns
        -------
        `str`
            A string as a timestamp. Hours, minutes and seconds are given.
        """
        return self.timestamp.strftime(r"[%H:%M:%S]")

    @property
    def log(self) -> str:
        """
        log function

        Returns the timestamp formatted for logging purposes.

        Returns
        -------
        `str`
            A string as a timestamp. Day and month, also hours, minutes and seconds are given.
        """
        return self.timestamp.strftime(r"[%d/%m %H:%M:%S]")

    @property
    def file(self) -> str:
        """
        file function

        Returns the timestamp formatted for file names purposes.

        Returns
        -------
        `str`
            A string as a timestamp usable for file names. Day and month,
            also hours, minutes and seconds are given.
        """
        return self.timestamp.strftime(r"%d_%m__%H_%M_%S")

    def __enter__(self):
        """
        __enter__ function

        The sole purpose of this function is to enable the ability of this
        class to be used in the `with-as` statement.

        Returns
        -------
        `Timestamp`
            Returns itself.
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        __exit__ function

        The sole purpose of this function is to enable the ability of this
        class to be used in the `with-as` statement.

        Deletes itself when exiting `with-as` statement.
        """
        del self

class CustomStdout:
    """
    A Custom `sys.stdout` class

    Basically, a new `stdout` is defined using `io.StringIO()` text buffer.
    `write()` and `flush()` functions from the buffer are redefined and
    customized.

    This class affects mostly on python's `print()` function.
    """
    def __init__(self) -> None:
        # The buffer for the sys.stdout.
        self.buffer = io.StringIO()

    def write(self, __s: str) -> None:
        """
        write function

        A redefined io buffer `write()` function.

        When the input has a line separator (`\n`) at the end,
        `flush()` function will be attempted.

        Parameters
        ----------
        __s : `str`
            The input string given by `sys.stdout`
        """
        try:
            self.buffer.write(__s)
        except TypeError:
            payload = None
            for encoding in ['utf-8', 'latin-1', 'ascii', 'cp1252']:
                try:
                    payload = __s.decode(encoding)
                    __s = payload
                    break
                except UnicodeDecodeError:
                    pass
            if payload is None:
                del __s
                __s = "__Error_while_trying_to_decode_this_bytes_object__\n"
        if __s.endswith("\n"):
            self.flush()

    def flush(self) -> None:
        """
        flush function

        A redefined io buffer `flush()` function.

        Format and print the io buffer's data to the original
        stdout stream, (`sys.__stdout__`). Then clean up the buffer.

        Also, logger is used to log all `print()` calls.
        """
        output = self.buffer.getvalue()
        # print(f"Flushing --> {[output]}", file=sys.__stdout__)
        if output:
            timestamp = Timestamp()
            print(f"{SGR.format(timestamp.terminal, SGR.Foreground.blue)} {output}", file=sys.__stdout__, end= "")
            # If logger level is set more than info level (20), then this log is not saved.
            try:
                codecs.charmap_encode(output, None, encoding_table) # Just for error prevention.
                logger.info("%s", remove_sgr(output[:-1]), extra={"end": ""})
            except Exception as exception: # pylint: disable=broad-exception-caught
                logger.info("%s", "Here should be a log, failed to log.", extra={"end": ""})
                error(exception, traceback.format_exc(), "Logger could not log.")


        # Clean up the buffer using io built-in methods.
        self.buffer.truncate(0)
        self.buffer.seek(0)

class CustomStderr:
    """
    A Custom `sys.stderr` class

    Basically, a new `stderr` is defined using `io.StringIO()` text buffer.
    `write()` and `flush()` functions from the buffer are redefined and
    customized.

    This class affects on all exceptions. Every exception raised will go
    trough this class.
    """
    def __init__(self) -> None:
        # The buffer for the sys.stderr.
        self.buffer = io.StringIO()

    def write(self, __s: str) -> None:
        """
        write function

        A redefined io buffer `write()` function.

        When the input has a line separator (`\n`) at the end,
        `flush()` function will be attempted.

        Parameters
        ----------
        __s : `str`
            The input string given by `sys.stderr`
        """

        self.buffer.write(__s)
        if __s.endswith("\n"):
            self.flush()

    def flush(self) -> None:
        """
        flush function

        A redefined io buffer `flush()` function.

        Format and print the io buffer's data to the original
        stderr stream, (`sys.__stderr__`). Then clean up the buffer.

        Also, logger is used to log all exception raisings.
        """
        output = self.buffer.getvalue()
        # print(f"Flushing --> {[output]}", file=sys.__stdout__)
        if output:
            timestamp = Timestamp()
            timestamp_preformat = f"{SGR.format(timestamp.terminal, SGR.Foreground.blue)} "
            # If its not the first line of a traceback...
            if not "Traceback" in output:
                # Adding a timestamp only to the first traceback's line.
                # Adding indention to the rest of the lines.
                timestamp_preformat = (" " * (1 + len(timestamp.terminal)))

            print(f"{timestamp_preformat}{SGR.format(output, SGR.Background.rgb(70, 30, 50))}", file=sys.__stdout__, end= "")
            try:
                codecs.charmap_encode(output, None, encoding_table) # Just for error prevention.
                logger.error("%s", remove_sgr(output[:-1]), extra={"end": ""})
            except Exception as exception: # pylint: disable=broad-exception-caught
                logger.info("%s", "Here should be a log, failed to log.", extra={"end": ""})
                error(exception, traceback.format_exc(), "Logger could not log.")

        self.buffer.truncate(0)
        self.buffer.seek(0)

def error(
    exception: Exception,
    traceback_format_exc: str,
    message: str | None = None,
    advice: str | None = None,
    level: str | int = "ERROR"
    ) -> None:
    """
    error function

    An error handler, useful to print errors with custom messages and advices.

    Parameters
    ----------
    exception : `Exception`
        Should be the exception itself.

        For example, `except ValueError as exception`: where `exception` should
        be this parameter.
    
    traceback_format_exc : `str`
        The traceback. Always should be a string.

        Is recommended to use `traceback.format_exc()` but it is not the only way.
        See traceback module's documentation for more information.
    
    message : `str | None, optional`
        A message that will be printed as a header of the error, by default None.
    
    advice : `str | None, optional`
        An advice that will be printed at the very end of the error, by default None.
    
    level : `str | int, optional`
        The logging level for this particular error, by default "ERROR".

        Can not be DEBUG level or any lower, otherwise `ValueError` exception is raised.

    Raises
    ------
    `ValueError`
        If the logging level is at DEBUG or any lower.
    `ValueError`
        If the logging level is set by using an undefined name.
    """
    timestamp = Timestamp()
    concatenator = str()

    # Creating a decorated concatenation between message and exception.
    # Only if there is a message.
    if message is None:
        message = ""
    else:
        concatenator = " --> "

    # Adding adive to the end of the traceback. Only if there is a advice.
    if advice is None:
        advice = ""
        traceback_output = traceback_format_exc
    else:
        traceback_output = " \n".join([traceback_format_exc, SGR.format(advice, SGR.Foreground.yellow)])

    # Translating logging level from string to integer.
    try:
        level = level.upper()
        match level:
            case "CRITICAL":
                level = 50
            case "ERROR":
                level = 40
            case "WARNING":
                level = 30
            case "INFO":
                level = 20
            case "DEBUG":
                try:
                    raise ValueError(level)
                except ValueError as sub_exception:
                    error(exception, traceback.format_exc(), message, advice)
                    return error(sub_exception, traceback.format_stack()[0],
                            "error() handler can not have DEBUG logging level or any lower",
                            "Use the logging debug() function on your current logger instead.",
                            level= "warning")
            case _:
                try:
                    raise ValueError(level)
                except ValueError as sub_exception:
                    error(sub_exception, traceback.format_exc(),
                            f"logging level {level} is unknown.",
                            f"If {level} is a custom logging level, try to set it as a integer.",
                            level= "warning")
                    return error(exception, traceback_format_exc, message, advice)
    except AttributeError:
        if level <= 10:
            try:
                raise ValueError(level) # pylint: disable=raise-missing-from
            except ValueError as sub_exception:
                error(sub_exception, traceback.format_exc(),
                        "error() handler can not have DEBUG logging level or any lower",
                        "Use the logging debug() function on your current logger instead.",
                        level= "warning")
                return error(exception, traceback_format_exc, message, advice)

    # Print the error and log it.
    print(f"{SGR.format(timestamp.terminal, SGR.Foreground.blue)} "+
          f"{SGR.EXT}{SGR.Foreground.red}{SGR.F}{message}{concatenator}"+
          f"{SGR.EXT}{SGR.BrightForeground.red}{SGR.F}{exception}{SGR.default_format}",
          file= sys.__stdout__)
    try:
        codecs.charmap_encode(message, None, encoding_table) # Just for error prevention.
        logger.log(level, "%s%s%s", message, concatenator, exception)
    except Exception as exception: # pylint: disable=unused-argument, broad-exception-caught
        logger.info("%s", "Here should be a log, failed to log.", extra={"end": ""})
        error(exception, traceback.format_exc(), "Logger could not log.")

    print(f"{SGR.format(timestamp.terminal, SGR.Foreground.blue)} "+
          f"{SGR.format(traceback_output, SGR.Background.rgb(70, 30, 50))}",
          file= sys.__stdout__)
    try:
        codecs.charmap_encode(traceback_format_exc, None, encoding_table) # Just for error prevention.
        logger.log(level, "%s\n%s", traceback_format_exc, advice)
    except Exception as exception: # pylint: disable=broad-exception-caught
        logger.info("%s", "Here should be a log, failed to log.", extra={"end": ""})
        error(exception, traceback.format_exc(), "Logger could not log.")

def initialize():
    global logger # pylint: disable=global-variable-undefined, invalid-name
    logging.config.fileConfig("config/terminal/logging.conf")
    logger = logging.getLogger("root")

    # Creates logs directory if it does not exist.
    os.makedirs("logs", exist_ok=True)

    # Attempts to create a unique logging file.
    with Timestamp() as timestamp:
        path = f"logs/{timestamp.file}.log"
        if os.path.exists(path):
            path = f"logs/{timestamp.file}_duplicate.log"

        number = 2
        while os.path.exists(path) and number < 1000:
            path = f"logs/{timestamp.file}_duplicate_{number}.log"
            number += 1

        if os.path.exists(path):
            try:
                raise IOError(path)
            except IOError as exception:
                error(exception, traceback.format_exc(), "Unable to create a logging file!?")

    # Defines the File Handler for the logger.
    handler = logging.FileHandler(path, "a")
    handler.formatter = logger.handlers[0].formatter
    handler.level = logger.handlers[0].level

    # Adds the File Handler to the logger.
    logger.removeHandler(logger.handlers[0])
    logger.addHandler(handler)

    # Changes the stream to the custom ones.
    sys.stdout = CustomStdout()
    sys.stderr = CustomStderr()

    match handler.level:
        case 50:
            handler_level= f"CRITICAL ({handler.level})"
        case 40:
            handler_level= f"ERROR ({handler.level})"
        case 30:
            handler_level= f"WARNING ({handler.level})"
        case 20:
            handler_level= f"INFO ({handler.level})"
        case 10:
            handler_level= f"DEBUG ({handler.level})"
        case 0:
            handler_level= f"NOTSET ({handler.level})"
        case _:
            handler_level= handler.level


    print(f"{SGR.set(SGR.Background.rgb(30, 60, 100), SGR.Foreground.magenta)}Loaded "+
        f"{SGR.set(SGR.Foreground.yellow)}{SGR.apply('Terminal Handler Module', SGR.bold)} "+
        f"{SGR.set(SGR.Foreground.magenta)}successfully, created by "+
        f"{SGR.set(SGR.Foreground.yellow)}{SGR.apply('TeRacksito', SGR.bold)}"+
        f"{SGR.set(SGR.Foreground.magenta)}!{SGR.default_format}")

    print(SGR.format(f"Logging at {SGR.apply(path, SGR.bold, SGR.underline)} "+
                    f"with {SGR.apply(handler_level, SGR.bold)} level!",
                    SGR.Foreground.rgb(128, 128, 128)))
