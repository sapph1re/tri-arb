import logging
import string


class GracefulStringFormatter(string.Formatter):
    def __init__(self, missing='NONE', bad_fmt='BADFORMAT'):
        self.missing = missing
        self.bad_fmt = bad_fmt

    def get_field(self, field_name, args, kwargs):
        # Handle a key not found
        try:
            val = super().get_field(field_name, args, kwargs)
        except (KeyError, AttributeError):
            val = None, field_name
        return val

    def format_field(self, value, spec):
        # handle an invalid format
        if value==None:
            return self.missing
        try:
            return super().format_field(value, spec)
        except ValueError:
            if self.bad_fmt is not None:
                return self.bad_fmt
            else:
                raise


string_formatter = GracefulStringFormatter()


class GracefulFormatter(logging.Formatter):
    def __init__(self, fmt=None, datefmt=None, style='{', missing='NONE', bad_fmt='BADFORMAT'):
        self.missing, self.bad_fmt = missing, bad_fmt
        super().__init__(fmt, datefmt, style)

    def formatMessage(self, record):
        return string_formatter.format(self._fmt, **record.__dict__)


class Message(object):
    def __init__(self, fmt, args):
        self.fmt = fmt
        self.args = args

    def __str__(self):
        return string_formatter.format(self.fmt, *self.args)


class StyleAdapter(logging.LoggerAdapter):
    def __init__(self, logger, extra=None):
        super(StyleAdapter, self).__init__(logger, extra or {})

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            self.logger._log(level, Message(msg, args), (), **kwargs)


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    #  format_krot = '%(asctime)s - %(name)s - %(levelname)s - %(filename)s[LINE:%(lineno)d]\n%(message)s\n'
    #  format_saph = '{asctime} {levelname} [{threadName}] [{name}:{funcName}] {message}'
    format_main = '{asctime}\t{levelname}\t[{filename} <> {funcName}() <> LineNo:{lineno}]\t{message}'
    format_time = '%H:%M:%S'
    log_formatter_debug = GracefulFormatter(format_main, format_time)
    handler_console = logging.StreamHandler()
    handler_console.setLevel(logging.DEBUG)
    handler_console.setFormatter(log_formatter_debug)
    logger.addHandler(handler_console)
    logger = StyleAdapter(logger)
    return logger
