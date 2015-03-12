from .consts import ERROR, WORK_FAIL, WORK_EXCEPTION


class GearmanException(Exception):
    """Rises whenever the server encounters an error and needs
    to notify a client or worker"""

    code = ERROR


class GearmanWorkFailException(GearmanException):
    """This is to notify the server (and any listening clients) that
    the job failed.
    """

    code = WORK_FAIL


class GearmanWorkException(GearmanException):
    """WORK_EXCEPTION

    This is to notify the server (and any listening clients) that
    the job failed with the given exception.
    """
    code = WORK_EXCEPTION
